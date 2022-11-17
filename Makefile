build:
	@echo "Building..."
	@gcc -o tema1 tema1.c -lm -Wall -Werror
	@echo "Done"

build_debug:
	@echo "Building debug..."
	@gcc -o tema1 tema1.c -lm -Wall -Werror -O0 -g3 -DDEBUG
	@echo "Done"

clean:
	@echo "Cleaning..."
	@rm -rf tema1
	@echo "Done"
