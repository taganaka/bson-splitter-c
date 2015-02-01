//
//  main.c
//  bison-splitter-c
//
//  Created by Francesco Laurita on 1/31/15.
//  Copyright (c) 2015 Francesco Laurita. All rights reserved.
//

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

typedef struct {
  int32_t       len;
  unsigned char *payload;
} bson_doc_hnd_t;


bson_doc_hnd_t* bson_decode(FILE *fp){
    
  unsigned char header[4];
  size_t read;
  read = fread(header, 1, 4, fp);
  bson_doc_hnd_t *ret_ptr;
  
  if (read != 4)
    return NULL;
  
  int32_t doc_size = *((int32_t*) header) - 4;
  
  if (doc_size < 5)
    return NULL;
  
  ret_ptr = malloc(sizeof(bson_doc_hnd_t));
  ret_ptr->payload = malloc(doc_size + 4);
  ret_ptr->len     = doc_size;
  
  memcpy(ret_ptr->payload, &header, 4);
  
  if (fread(ret_ptr->payload + 4, 1, doc_size, fp) != doc_size){
    free(ret_ptr->payload);
    free(ret_ptr);
    return NULL;
  }
  
  return ret_ptr;
}

int main(int argc, const char * argv[]) {
    
  FILE *fp, *ofp;
  size_t num_doc = 0, bytes_written = 0, current_doc_num = 0;
  int num_split = 1;
  char fname[255];
  size_t fsize = 10 * 1024 * 1024;
  bson_doc_hnd_t *doc_ptr;
  
  fp = fopen(argv[1],"rb");
  
  if (!fp)
    return EXIT_FAILURE;
  
  sprintf(fname, "split-%d.bson", num_split);
  ofp = fopen(fname, "wb");
  
  while (1) {
  
    doc_ptr = bson_decode(fp);

    if (doc_ptr == NULL)
      break;
    
    num_doc++;
    current_doc_num++;
    bytes_written += fwrite(doc_ptr->payload, sizeof(unsigned char), doc_ptr->len + 4, ofp);

    if (bytes_written > fsize) {
      num_split++;
      fflush(ofp);
      fclose(ofp);
      printf("[%s] bytes written: %ld docs dumped: %ld\n", fname, bytes_written, current_doc_num);
      sprintf(fname, "split-%d.bson", num_split);
      bytes_written   = 0;
      current_doc_num = 0;
      ofp = fopen(fname, "wb");
    }
    
    free(doc_ptr->payload);
    free(doc_ptr);
  }
  
  if (ofp) {
    fflush(ofp);
    fclose(ofp);
    printf("[%s] bytes written: %ld docs dumped: %ld\n", fname, bytes_written, current_doc_num);
  }

  fclose(fp);
  return EXIT_SUCCESS;
}
