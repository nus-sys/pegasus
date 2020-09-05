all: endhost

clean:
	$(MAKE) -C emulation/ clean

endhost:
	$(MAKE) -C emulation
