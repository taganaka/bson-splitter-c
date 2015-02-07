//
//  bson-splitter-c
//
//  Created by Francesco Laurita on 1/31/15.
//  Copyright (c) 2015 Francesco Laurita <francesco.laurita@gmail.com>. All rights reserved.
//
#define _GNU_SOURCE
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <ctype.h>
#include <stdbool.h>

#define OFFSET 4
#define DEFAULT_SPLIT_NAME "split-%d.bson"

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
    doc_size =  ((header[3] << 24) | (header[2] << 16) | (header[1] << 8) | header[0]) - OFFSET;

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

FILE* mk_fileoutput(char **current_split_name, const char *fmask, const int num_split) {
    if (asprintf(current_split_name, fmask, num_split) == -1) {
        fprintf(stderr, "Unable allocate new file name\n");
        return false;
    }
    return fopen(*current_split_name, "wb");
}


void usage() {

    fprintf(stderr, "Usage:\n");
    fprintf(stderr, "  bson-splitter [OPTIONS] <bson file> <split size in MB>\n");
    fprintf(stderr, "  Es: ./bson-splitter /backup/huge.bson 250\n");
}


int main(int argc, char * argv[]) {

    char *fmask = NULL;
    char *xpopen = NULL;
    FILE *fp = NULL, *ofp = NULL;
    size_t num_doc = 0, bytes_written = 0, current_doc_num = 0;
    int num_split = 1;
    char *current_split_name = NULL;
    char *current_xopen_cmd  = NULL;
    size_t fsize;
    int size_in_mb = 0;
    bson_doc_hnd_t *doc_ptr;

    int c;
    while ((c = getopt (argc, argv, "f:X:")) != -1) {
        switch (c) {
        case 'f':
            if (strstr(optarg, "%d") == NULL) {
                fprintf(stderr, "Options -f requires '%%d' somewhere. Got %s\n", optarg);
                usage();
                return EXIT_FAILURE;
            }
            fmask = optarg;
            break;
        case 'X':
            xpopen = optarg;
            printf("%s\n", xpopen);
            break;
        case '?':
            if (optopt == 'f' || optopt == 'X')
                fprintf (stderr, "Option -%c requires an argument.\n", optopt);
            else if(isprint (optopt))
                fprintf(stderr, "Unknown option `-%c'.\n", optopt);
            else
                fprintf(stderr, "Unknown option character `\\x%x'.\n", optopt);
        default:
            usage();
            return EXIT_FAILURE;
        }
    }


    if (fmask == NULL) {
        fmask = DEFAULT_SPLIT_NAME;
    }


    if (argv[optind] == NULL || argv[optind + 1] == NULL) {
        usage();
        return EXIT_FAILURE;
    }

    int i;
    for (i = 0; i < (argc - optind); ++i) {
        switch (i) {
        case 0:
            if (strcmp(argv[i + optind], "-") == 0) {
                fp = stdin;
            } else {
                fp = fopen(argv[i + optind],"rb");
            }

            if (!fp) {
                fprintf(stderr, "Can't open %s for reading\n", argv[i + optind]);
                usage();
                return EXIT_FAILURE;
            }
            break;
        case 1:
            if (sscanf (argv[i + optind], "%i", &size_in_mb) != 1) {
                fprintf(stderr, "Error: %s is not an integer\n", argv[i + optind]);
                usage();
                return EXIT_FAILURE;
            }

            if (size_in_mb <= 0) {
                fprintf(stderr, "Error: %d needs to be a positive value\n", size_in_mb);
                usage();
                return EXIT_FAILURE;
            }
            break;
        default:
            fprintf(stderr,"Too many positional argumemts\n");
            if (fp)
                fclose(fp);
            usage();
            return EXIT_FAILURE;
        }
    }

    fsize = size_in_mb * 1024 * 1024;

    ofp = mk_fileoutput(&current_split_name, fmask, num_split);



    // if (xpopen != NULL) {
    //     if (asprintf(&current_xopen_cmd, xpopen, current_split_name) == -1) {
    //         fprintf(stderr, "Unable allocate new popen arg\n");
    //         return EXIT_FAILURE;
    //     }
    //     ofp = popen(current_xopen_cmd, "wb");
    // } else {
    //     ofp = fopen(current_split_name, "wb");
    // }


    if (!ofp) {
        fprintf(stderr, "Unable to open %s for writing\n", current_split_name);
        return EXIT_FAILURE;
    }

    while (1) {

        doc_ptr = bson_decode(fp);

        if (doc_ptr == NULL)
            break;

        num_doc++;
        current_doc_num++;
        bytes_written += fwrite(doc_ptr->payload, sizeof(unsigned char), doc_ptr->payload_size, ofp);
        fflush(ofp);

        if (bytes_written > fsize) {
            num_split++;
            fflush(ofp);
            fclose(ofp);
            printf("[%s] bytes written: %ld docs dumped: %ld\n", current_split_name, bytes_written, current_doc_num);
            free(current_split_name);
            ofp = mk_fileoutput(&current_split_name, fmask, num_split);
            if (!ofp) {
                fprintf(stderr, "Unable to open %s for writing\n", current_split_name);
                return EXIT_FAILURE;
            }
            bytes_written   = 0;
            current_doc_num = 0;
        }

        free(doc_ptr->payload);
        free(doc_ptr);

    }

    if (ofp) {
        fflush(ofp);
        fclose(ofp);
        printf("[%s] bytes written: %ld docs dumped: %ld\n", current_split_name, bytes_written, current_doc_num);
    }

    free(current_split_name);
    fclose(fp);

    if (num_doc > 0) {
        printf("%ld documents dumped into %d splits\n", num_doc, num_split);
    }
    return EXIT_SUCCESS;
}
