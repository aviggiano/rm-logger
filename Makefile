logger: 
	gcc -Werror -fPIC -std=gnu99 -c -o logger.o logger.c
	ld -o logger.so logger.o -shared -Bsymbolic -lc


