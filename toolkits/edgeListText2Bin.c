#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>
#include <errno.h>
#include <stdint.h>
#include <inttypes.h>
#include <assert.h>
#include <limits.h>

struct thread_info {
	unsigned ID;
	/* In edges */
	unsigned initial_offset;
	unsigned edges_to_process;
	/* Initial byte offset in output file */
	char *output_buf;
	/* Parsing flags */
	char weighted;
	char one_indexed;
	char gen_weights;
	char *input_file;
	unsigned long max_vID;
	/* Used by rand_r for random weight generation */
	unsigned *thread_seed;
};

char *copy_string(char *input)
{
	char *ret;
	size_t len;

	if(!input)
		return NULL;

	len = strlen(input) + 1;
	ret = malloc(len);

	if(!ret){
		perror("malloc");
		return NULL;
	}

	ret = strcpy(ret, input);

	return ret;
}

void thread_cleanup(void *arg)
{
	FILE *fp = (FILE *)arg;
	if(fp)
		fclose(fp);
}

void *edge_parsing_thread(void *arg)
{
	struct thread_info *info = (struct thread_info *)arg;
	int old_cancel_state;
	FILE *thread_fp = NULL;
	unsigned long j;
	char *line_read = NULL;
	size_t line_size;
	char *iter = info->output_buf;

	/* Defer cancellation until the cleanup function is properly setup */
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &old_cancel_state);
	pthread_cleanup_push(thread_cleanup, (void *)thread_fp);
	pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, &old_cancel_state);

	thread_fp = fopen(info->input_file, "r");

	if(!thread_fp){
		fprintf(stderr, "Thread %u error on fopen\n", info->ID + 1);
		pthread_exit(NULL);
	}
	/* 
	 * Seek to starting offset.
	 * This loop, along with the actual
	 * edge parsing loop assumes no comment or
	 * blank lines. Add some checks for that.
	 */
	for(j = 0; j < info->initial_offset; j++){
		if(getline(&line_read, &line_size, thread_fp) == -1){
			fprintf(stderr, "Thread %u error on getline for line %lu\n",
					info->ID + 1, j + 1);
			fclose(thread_fp);
			pthread_exit(NULL);
		}
		free(line_read);
		line_read = NULL;
	}
	/* Now actually use the lines you read */
	for(j = 0; j < info->edges_to_process; j++){
		unsigned long src = 0, dst = 0;
		float weight = 0.0;

		if(getline(&line_read, &line_size, thread_fp) == -1){
			fprintf(stderr, "Thread %u error on getline for line %lu\n",
					info->ID, info->initial_offset + j + 1);
			fclose(thread_fp);
			pthread_exit(NULL);
		}

		if(info->weighted)
			sscanf(line_read, "%lu%lu%f", &src, &dst, &weight);
		else
			sscanf(line_read, "%lu%lu", &src, &dst);

		if(info->one_indexed == 1){
			/* Gemini is 0-based, adjust */
			if(!src || !dst)
				printf("WARNING: 1-indexed flag but <%lu, %lu> found on line %lu!\n",
						src, dst, info->initial_offset + j + 1);
			src--;
			dst--;
		}
		if(src == ULONG_MAX || dst == ULONG_MAX)
			printf("WARNING: weird edge <%lu, %lu>, line %lu\n",
					src, dst, info->initial_offset + j + 1);
		/* Adjust max vertex ID */
		if(src > info->max_vID)
			info->max_vID = src;
		if(dst > info->max_vID)
			info->max_vID = dst;

		memcpy(iter, &src, sizeof(unsigned long));
		iter += sizeof(unsigned long);
		memcpy(iter, &dst, sizeof(unsigned long));
		iter += sizeof(unsigned long);
		if(info->weighted){
			memcpy(iter, &weight, sizeof(float));
			iter += sizeof(float);
		}else if(info->gen_weights){
			/* Random float weight in [0, 1) */
			weight = ((float)rand_r(info->thread_seed)) / RAND_MAX;
			memcpy(iter, &weight, sizeof(float));
			iter += sizeof(float);
		}
		
		free(line_read);
		line_read = NULL;
	}
	fclose(thread_fp);
	fprintf(stderr, "Thread %u done.\n", info->ID + 1);
	pthread_cleanup_pop(0);
	return NULL;
}

int main(int argc, char *argv[])
{
	int ret = 0, i, opt, output_fd = -1;
	unsigned threads = 32;
	unsigned *thread_seeds = NULL;
	unsigned long edges = 1000, edges_per_thread, max_vID = 0UL;
	char *input_path = NULL, *output_path = NULL, *strend;
	char weighted_graph = 0;
	char one_indexed = 0;
	char generate_weights = 0;
	pthread_t *thread_arr = NULL;
	struct thread_info *info_arr = NULL;
	off_t output_file_size;
	void *map;
	size_t size_per_edge;

	assert(sizeof(long unsigned) == sizeof(uint64_t));

	while((opt = getopt(argc, argv, "t:e:f:o:waih")) != -1){
		switch(opt) {
			case 't':
				threads = atoi(optarg);
				if(!threads){
					fprintf(stderr, "Cannot operate with 0 threads\n");
					exit(EXIT_FAILURE);
				}
				break;
			case 'e':
				errno = 0;
				edges = strtoul(optarg, &strend, 10);
				if(errno != 0){
					perror("strtoul");
					exit(EXIT_FAILURE);
				}
				break;
			case 'f':
				input_path = copy_string(optarg);
				if(!input_path)
					exit(EXIT_FAILURE);
				break;
			case 'o':
				output_path = copy_string(optarg);
				if(!output_path)
					exit(EXIT_FAILURE);
				break;
			case 'w':
				if(generate_weights){
					fprintf(stderr, "Cannot generate weights for weighted input graph!\n");
					exit(EXIT_FAILURE);
				}
				weighted_graph = 1;
				break;
			case 'a':
				if(weighted_graph){
					fprintf(stderr, "Cannot generate weights for weighted input graph!\n");
					exit(EXIT_FAILURE);
				}
				generate_weights = 1;
				break;

			case 'i':
				one_indexed = 1;
				break;
			case 'h':
				fprintf(stderr, "Usage: %s -t <num_threads> -e <num_edges> -f <input file path> -o <output file path> [-w: Indicate weighted input] [-a: Generate random weights] [-i: Indicate 1-indexed edge list]\n",
						argv[0]);
				exit(0);
			default:
				fprintf(stderr, "Usage: %s -t <num_threads> -e <num_edges> -f <input file path> -o <output file path> [-w: Indicate weighted input] [-a: Generate random weights] [-i: Indicate 1-indexed edge list]\n",
						argv[0]);
				exit(EXIT_FAILURE);
				break;
		}
	}

	if(!input_path){
		input_path = copy_string("./input_graph");
		if(!input_path){
			ret = EXIT_FAILURE;
			goto out;
		}
	}

	if(!output_path){
		output_path = copy_string("./output_graph");
		if(!output_path){
			ret = EXIT_FAILURE;
			goto out;
		}
	}

	if(generate_weights){
		int randfd;
		thread_seeds = (unsigned *)malloc(threads * sizeof(unsigned));
		if(!thread_seeds){
			perror("malloc");
			ret = EXIT_FAILURE;
			goto out;
		}
		/* Open /dev/urandom and read one unsigned seed per thread */
		randfd = open("/dev/urandom", O_RDONLY);
		if(randfd == -1){
			perror("open");
			ret = EXIT_FAILURE;
			goto out;
		}
		if(read(randfd, thread_seeds, threads * sizeof(unsigned)) != threads * sizeof(unsigned)){
			fprintf(stderr, "Read error/insufficient bytes /dev/urandom\n");
			ret = EXIT_FAILURE;
			close(randfd);
			goto out;
		}
		close(randfd);
	}

	thread_arr = (pthread_t *)malloc(threads * sizeof(pthread_t));
	if(!thread_arr){
		perror("malloc");
		ret = EXIT_FAILURE;
		goto out;
	}

	info_arr = (struct thread_info *)malloc(threads * sizeof(struct thread_info));
	if(!info_arr){
		perror("malloc");
		ret = EXIT_FAILURE;
		goto out;
	}

	output_fd = open(output_path, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);

	if(output_fd == -1){
		perror("open");
		ret = EXIT_FAILURE;
		goto out;
	}

	size_per_edge = weighted_graph ? (2 * sizeof(unsigned long) + sizeof(float)) : (2 * sizeof(unsigned long));
	output_file_size = edges * size_per_edge;

	printf("|E| = %lu, bytes per edge: %lu (0x%lx), output file size: %lu bytes\n",
			edges, size_per_edge, size_per_edge, output_file_size);

	printf("Fallocating...\n");
	if(posix_fallocate(output_fd, 0, output_file_size) == -1){
		perror("posix_fallocate");
		ret = EXIT_FAILURE;
		close(output_fd);
		goto out;
	}

	map = mmap(NULL, output_file_size, PROT_READ | PROT_WRITE, MAP_SHARED, output_fd, 0);
	if(map == MAP_FAILED){
		perror("mmap");
		ret = EXIT_FAILURE;
		close(output_fd);
		goto out;
	}
	madvise(map, output_file_size, MADV_SEQUENTIAL);

	edges_per_thread = edges / threads;
	
	printf("Spawning threads...\n");
	for(i = 0; i < threads; i++){
		info_arr[i].ID = (unsigned)i;
		info_arr[i].initial_offset = i * edges_per_thread;
		info_arr[i].edges_to_process = edges_per_thread;
		if(i == threads - 1)
			info_arr[i].edges_to_process += edges % threads;
		info_arr[i].weighted = weighted_graph;
		info_arr[i].gen_weights = generate_weights;
		info_arr[i].thread_seed = generate_weights ? (&thread_seeds[i]) : NULL;
		info_arr[i].one_indexed = one_indexed;
		info_arr[i].input_file = input_path;
		info_arr[i].output_buf = (char *)map + (i * edges_per_thread * size_per_edge);
		info_arr[i].max_vID = 0UL;
		if(pthread_create(&thread_arr[i], NULL, edge_parsing_thread,
					(void *)&info_arr[i]) != 0){
			int j;
			perror("pthread_create");
			ret = EXIT_FAILURE;
			for(j = 0; j < i; j++)
				pthread_cancel(thread_arr[j]);
			goto out;
		}
	}

	/* Wait for threads to finish */
	for(i = 0; i < threads; i++)
		pthread_join(thread_arr[i], NULL);

	printf("Threads done, msyncing...\n");

	msync(map, output_file_size, MS_SYNC);

	if(munmap(map, output_file_size) == -1){
		perror("munmap");
		ret = EXIT_FAILURE;
		close(output_fd);
		goto out;
	}

	/* Reduce max vertex IDs from threads */
	for(i = 0; i < threads; i++)
		if(info_arr[i].max_vID > max_vID)
			max_vID = info_arr[i].max_vID;
	printf("Maximum Vertex ID: %lu\n",max_vID);

	close(output_fd);
out:
	if(input_path)
		free(input_path);
	if(output_path)
		free(output_path);
	if(thread_seeds)
		free(thread_seeds);
	if(thread_arr)
		free(thread_arr);
	if(info_arr)
		free(info_arr);
	exit(ret);
}
