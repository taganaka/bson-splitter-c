//
//  bson-splitter-c
//
//  Created by Francesco Laurita on 1/31/15.
//  Copyright (c) 2015 Francesco Laurita <francesco.laurita@gmail.com>. All rights reserved.
//

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#define OFFSET 4

typedef struct {
    int32_t       len;
    int32_t       payload_size;
    unsigned char *payload;
} bson_doc_hnd_t;

size_t read_fully(unsigned char*, size_t, size_t, FILE*);
bson_doc_hnd_t* bson_decode(FILE*);

bson_doc_hnd_t* bson_decode(FILE *fp) {

    unsigned char header[OFFSET];
    int32_t doc_size;
    size_t r;

    bson_doc_hnd_t *ret_ptr;

    if ((r = read_fully(header, 0, OFFSET, fp)) != OFFSET) {
        if (r == 0) { // eof
            return NULL;
        }
        printf("ERROR: expected %d, got %ld. Corrupted BSON file?\n", OFFSET, r);
        return NULL;
    }

    // Doc len is stored into the first 4 bytes
    // Doc len includes first 4 bytes as well
    doc_size = *((int32_t*) header) - OFFSET;

    if (doc_size < 5) {
        printf("ERROR: unable to read document size (%d). Corrupted BSON file?\n", doc_size);
        return NULL;
    }

    ret_ptr = (bson_doc_hnd_t *)malloc(sizeof(bson_doc_hnd_t));
    if (ret_ptr == NULL) {
        printf("ERROR: malloc failed\n");
        return NULL;
    }
    ret_ptr->payload      = (unsigned char*)malloc(doc_size + OFFSET);
    if (ret_ptr->payload == NULL) {
        free(ret_ptr);
        printf("ERROR: malloc failed\n");
        return NULL;
    }
    ret_ptr->len          = doc_size;
    ret_ptr->payload_size = doc_size + OFFSET;

    memcpy(ret_ptr->payload, &header, OFFSET);

    if ((r = read_fully(ret_ptr->payload, OFFSET, doc_size, fp)) != doc_size) {
        printf("ERROR: expected %d, got %ld. Corrupted BSON file?\n", doc_size, r);
        free(ret_ptr->payload);
        free(ret_ptr);
        return NULL;
    }

    return ret_ptr;
}

size_t read_fully(unsigned char *dest, size_t start, size_t len, FILE *fp) {

    size_t goal = len;
    size_t tot  = 0;
    size_t read = 0;

    while (goal > 0) {

        read = fread(dest + start, 1, len, fp);
        if (!read)
            return tot;

        goal  -= read;
        start += read;
        tot   += read;
    }
    return tot;
}

void usage() {

    printf("Usage:\n");
    printf("  bson-splitter <bson file> <split size in MB>\n");
    printf("  Es: ./bson-splitter /backup/huge.bson 250\n");
}

int main(int argc, const char * argv[]) {

    if (argc < 3) {
        usage();
        return EXIT_FAILURE;
    }

    FILE *fp, *ofp;
    size_t num_doc = 0, bytes_written = 0, current_doc_num = 0;
    int num_split = 1;
    char fname[255];
    size_t fsize;
    int size_in_mb;
    bson_doc_hnd_t *doc_ptr;

    if (strcmp(argv[1], "-") == 0) {
        fp = stdin;
    } else {
        fp = fopen(argv[1],"rb");
    }


    if (!fp) {
        printf("Can't open %s for reading\n", argv[1]);
        usage();
        return EXIT_FAILURE;
    }

    if (sscanf (argv[2], "%i", &size_in_mb) != 1) {
        printf("Error: %s not an integer\n", argv[2]);
        usage();
        return EXIT_FAILURE;
    }

    if (size_in_mb <= 0) {
        printf("Error: %d needs to be a positive value\n", size_in_mb);
        usage();
        return EXIT_FAILURE;
    }

    fsize = size_in_mb * 1024 * 1024;

    sprintf(fname, "split-%d.bson", num_split);
    ofp = fopen(fname, "wb");

    while (1) {

        doc_ptr = bson_decode(fp);

        if (doc_ptr == NULL)
            break;

        num_doc++;
        current_doc_num++;
        bytes_written += fwrite(doc_ptr->payload, sizeof(unsigned char), doc_ptr->payload_size, ofp);

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

    if (num_doc > 0) {
        printf("%ld documents dumped into %d splits\n", num_doc, num_split);
    }
    return EXIT_SUCCESS;
}
