build: tema1.c tema1.h linked_list.c linked_list.h process_files.c process_files.h
	@echo "Building..."
	@gcc -o tema1 tema1.c linked_list.c process_files.c -lm -lpthread -Wall 
	@echo "Done"

clean:
	@echo "Cleaning..."
	@rm -rf tema1 out*
	@echo "Done"
