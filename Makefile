build:
	@echo "Building..."
	@gcc -o a.out main.c -lm -Wall -Werror
	@echo "Done"

build_debug:
	@echo "Building debug..."
	@gcc -o a.out main.c -lm -Wall -Werror -O0 -g3 -DDEBUG
	@echo "Done"

clean:
	@echo "Cleaning..."
	@rm -rf a.out
	@echo "Done"