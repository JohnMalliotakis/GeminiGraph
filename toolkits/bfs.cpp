/*
Copyright (c) 2014-2015 Xiaowei Zhu, Tsinghua University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include <stdio.h>
#include <stdlib.h>
#include <random>

#include "core/graph.hpp"

void compute(Graph<Empty> * graph, VertexId root) {
  double exec_time = 0;
  exec_time -= get_time();

  VertexId * parent = graph->alloc_vertex_array<VertexId>();
  VertexSubset * visited = graph->alloc_vertex_subset();
  VertexSubset * active_in = graph->alloc_vertex_subset();
  VertexSubset * active_out = graph->alloc_vertex_subset();

  visited->clear();
  visited->set_bit(root);
  active_in->clear();
  active_in->set_bit(root);
  graph->fill_vertex_array(parent, graph->vertices);
  parent[root] = root;

  VertexId active_vertices = 1;

  for (int i_i=0;active_vertices>0;i_i++) {
    if (graph->partition_id==0) {
      printf("active(%d)>=%lu\n", i_i, active_vertices);
    }
    active_out->clear();
    active_vertices = graph->process_edges<VertexId,VertexId>(
      [&](VertexId src){
        graph->emit(src, src);
      },
      [&](VertexId src, VertexId msg, VertexAdjList<Empty> outgoing_adj){
        VertexId activated = 0;
        for (AdjUnit<Empty> * ptr=outgoing_adj.begin;ptr!=outgoing_adj.end;ptr++) {
          VertexId dst = ptr->neighbour;
          if (parent[dst]==graph->vertices && cas(&parent[dst], graph->vertices, src)) {
            active_out->set_bit(dst);
            activated += 1;
          }
        }
        return activated;
      },
      [&](VertexId dst, VertexAdjList<Empty> incoming_adj) {
        if (visited->get_bit(dst)) return;
        for (AdjUnit<Empty> * ptr=incoming_adj.begin;ptr!=incoming_adj.end;ptr++) {
          VertexId src = ptr->neighbour;
          if (active_in->get_bit(src)) {
            graph->emit(dst, src);
            break;
          }
        }
      },
      [&](VertexId dst, VertexId msg) {
        if (cas(&parent[dst], graph->vertices, msg)) {
          active_out->set_bit(dst);
          return 1;
        }
        return 0;
      },
      active_in, visited
    );
    active_vertices = graph->process_vertices<VertexId>(
      [&](VertexId vtx) {
        visited->set_bit(vtx);
        return 1;
      },
      active_out
    );
    std::swap(active_in, active_out);
  }

  exec_time += get_time();
  if (graph->partition_id==0) {
    printf("exec_time=%lf(s)\n", exec_time);
  }

  graph->gather_vertex_array(parent, 0);
  if (graph->partition_id==0) {
    VertexId found_vertices = 0;
    for (VertexId v_i=0;v_i<graph->vertices;v_i++) {
      if (parent[v_i] < graph->vertices) {
        found_vertices += 1;
      }
    }
    printf("found_vertices = %lu\n", found_vertices);
  }

  graph->dealloc_vertex_array(parent);
  delete active_in;
  delete active_out;
  delete visited;
}

int main(int argc, char ** argv) {
  MPI_Instance mpi(&argc, &argv);
  char *end;
  VertexId root;
  int threads;

  if (argc<4) {
    printf("bfs <threads> <file> <vertices> [source]\n");
    exit(-1);
  }

  threads = std::atoi(argv[1]);
  assert(threads > 0);

  Graph<Empty> * graph;
  graph = new Graph<Empty>(threads);

  VertexId vertices = std::strtoul(argv[3], &end, 10);
  end = NULL;
  graph->load_directed(argv[2], vertices);

  if (argc >= 5) {
	  root = std::strtoul(argv[4], &end, 10);
  } else {
  	  // Setup random number generation for BFS source
  	  std::random_device rdev;
  	  std::mt19937 gen(rdev()); // Seed for random generation
  	  std::uniform_int_distribution<unsigned long> udist(0, vertices - 1);
  	  VertexId root = udist(gen);
  	  // All MPI hosts must have the same source
  	  // Just choose the largest random number selected
  	  // across all machines
  	  MPI_Allreduce(MPI_IN_PLACE, &root, 1, MPI_UNSIGNED_LONG, MPI_MAX, MPI_COMM_WORLD);

          printf("Using randomly generated source vertex %lu\n", root);
  }

  compute(graph, root);
  for (int run=0;run<5;run++) {
    compute(graph, root);
  }

  delete graph;
  return 0;
}
