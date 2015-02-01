CC=gcc
CFLAGS=-Wall -O2
CLIBS=
OUT=bson-splitter
OBJS=bson-splitter.o


$(OUT): bson-splitter.o
	$(CC) $(CFLAGS) $(CLIBS) -o $(OUT) $(OBJS)
bson-splitter.o: bson-splitter.c
	$(CC) $(CFLAGS) -c bson-splitter.c -o bson-splitter.o

.PHONY: clean

clean:
	rm -rf *.o $(OUT)