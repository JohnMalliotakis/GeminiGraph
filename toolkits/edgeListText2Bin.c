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

/*
 * Struct used for binary
 * representation of edge info,
 * according to Gemini specifications
 */
struct edge_info {
	unsigned src;
	unsigned dst;
	/* Optional */
	float edge_weight;
};

struct thread_info {
	unsigned ID;
	unsigned initial_offset; /* In edges */
	unsigned edges_to_process;
	/* edges_to_process - sized output buffer */
	struct edge_info *output_buf;
	char weighted;
	char *input_file;
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

	ret = strncpy(ret, input, len);

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

	/* Defer cancellation until the cleanup function is properly setup */
	pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &old_cancel_state);
	pthread_cleanup_push(thread_cleanup, (void *)thread_fp);
	pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, &old_cancel_state);

	thread_fp = fopen(info->input_file, "r");

	if(!thread_fp){
		fprintf(stderr, "Thread %u error on fopen\n", info->ID);
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
					info->ID, j);
			fclose(thread_fp);
			pthread_exit(NULL);
		}
		free(line_read);
		line_read = NULL;
	}
	/* Now actually use the lines you read */
	for(j = 0; j < info->edges_to_process; j++){
		if(getline(&line_read, &line_size, thread_fp) == -1){
			fprintf(stderr, "Thread %u error on getline for line %lu\n",
					info->ID, j);
			fclose(thread_fp);
			pthread_exit(NULL);
		}
		if(info->weighted)
			sscanf(line_read, "%u%u%f", &info->output_buf[j].src,
					&info->output_buf[j].dst,
					&info->output_buf[j].edge_weight);
		else
			sscanf(line_read, "%u%u", &info->output_buf[j].src,
					&info->output_buf[j].dst);
		/* MTX format is 1-based, Gemini is 0-based, adjust */
		info->output_buf[j].src--;
		info->output_buf[j].dst--;
		printf("Thread %u read edge %u - %u at offset %lu\n",
				info->ID, info->output_buf[j].src, info->output_buf[j].dst,
				info->initial_offset + j);
		free(line_read);
		line_read = NULL;
	}
	fclose(thread_fp);
	fprintf(stderr, "Thread %u done.\n", info->ID);
	pthread_cleanup_pop(0);
	return NULL;
}

int main(int argc, char *argv[])
{
	int ret = 0, i, opt, output_fd = -1;
	unsigned threads = 32;
	unsigned long edges = 1000, edges_per_thread;
	char *input_path = NULL, *output_path = NULL, *strend;
	char weighted_graph = 0;
	pthread_t *thread_arr = NULL;
	struct thread_info *info_arr = NULL;
	struct edge_info *output_buffer = NULL;
	off_t output_file_size;
	void *map;

	while((opt = getopt(argc, argv, "t:e:f:o:wh")) != -1){
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
				weighted_graph = 1;
				break;
			case 'h':
				fprintf(stderr, "Usage: ./adjGraph2Binary -t <num_threads> -e <num_edges> -f <input file path> -o <output file path> [-w: Indicate weighted input]\n");
				exit(0);
			default:
				fprintf(stderr, "Usage: ./adjGraph2Binary -t <num_threads> -e <num_edges> -f <input file path> -o <output file path> [-w: Indicate weighted input]\n");
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

	/* Naive allocation, could fail for very large graphs */
	output_buffer = (struct edge_info *)malloc(edges * sizeof(struct edge_info));
	if(!output_buffer){
		perror("malloc");
		ret = EXIT_FAILURE;
		goto out;
	}

	edges_per_thread = edges / threads;
	
	for(i = 0; i < threads; i++){
		info_arr[i].ID = (unsigned)i;
		info_arr[i].initial_offset = i * edges_per_thread;
		info_arr[i].edges_to_process = edges_per_thread + ((i == threads - 1) ? (edges % threads) : 0);
		info_arr[i].weighted = weighted_graph;
		info_arr[i].input_file = input_path;
		info_arr[i].output_buf = &output_buffer[info_arr[i].initial_offset];
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


	output_fd = open(output_path, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);

	if(output_fd == -1){
		perror("open");
		ret = EXIT_FAILURE;
		goto out;
	}
	output_file_size = edges * (weighted_graph ? sizeof(struct edge_info) :
			(sizeof(struct edge_info) - sizeof(float)));

	if(posix_fallocate(output_fd, 0, output_file_size) == -1){
		perror("posix_fallocate");
		ret = EXIT_FAILURE;
		close(output_fd);
		goto out;
	}
#if 0
	if(weighted_graph){
		write(output_fd, (void *)output_buffer,
				edges * sizeof(struct edge_info));
	}else{
		unsigned long j;
		for(j = 0; j < edges; j++)
			write(output_fd, (void *)&output_buffer[j],
					sizeof(struct edge_info) -
					sizeof(float));
	}
	fsync(output_fd);
#endif
	/* 
	 * Use memory mapped I/O to take advantage of THP for sequential
	 * access pattern
	 */
	map = mmap(NULL, output_file_size, PROT_READ | PROT_WRITE, MAP_SHARED, output_fd, 0);
	if(map == MAP_FAILED){
		perror("mmap");
		ret = EXIT_FAILURE;
		close(output_fd);
		goto out;
	}
	madvise(map, output_file_size, MADV_SEQUENTIAL);

	if(weighted_graph){
		/* One huge memcpy */
		memcpy(map, output_buffer, edges * sizeof(struct edge_info));
	}else{
		unsigned long j;
		char *iter = (char *)map;
		for(j = 0; j < edges; j++){
			memcpy(iter, &output_buffer[j], 2 * sizeof(uint32_t));
			iter += 2 * sizeof(uint32_t);
		}
	}

	msync(map, output_file_size, MS_SYNC);

	if(munmap(map, output_file_size) == -1){
		perror("munmap");
		ret = EXIT_FAILURE;
		close(output_fd);
		goto out;
	}

	close(output_fd);
out:
	if(input_path)
		free(input_path);
	if(output_path)
		free(output_path);
	if(thread_arr)
		free(thread_arr);
	if(info_arr)
		free(info_arr);
	if(output_buffer)
		free(output_buffer);
	exit(ret);
}
