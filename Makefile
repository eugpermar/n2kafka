BIN=	n2kafka

SRCS=	engine.c global_config.c kafka.c n2kafka.c in_addr_list.c http.c \
		socket.c version.c
OBJS=	$(SRCS:.c=.o)

.PHONY:

all: $(BIN)

include mklove/Makefile.base

.PHONY: version.c

version.c: 
	@rm -f $@
	@echo "const char *n2kafka_revision=\"`git describe --abbrev=6 --dirty --tags --always`\";" >> $@
	@echo 'const char *n2kafka_version="1.0.0";' >> $@

install: bin-install

clean: bin-clean

-include $(DEPS)
