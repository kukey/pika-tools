# Top level makefile, the real shit is at src/Makefile

depends:
	git submodule update --init --recursive --force

default: all

.DEFAULT:
	cd pika_port_3 && $(MAKE) $@

.PHONY: depends
