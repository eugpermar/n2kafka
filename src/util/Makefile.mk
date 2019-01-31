THIS_SRCS := \
	file.c \
	in_addr_list.c \
	kafka.c \
	kafka_message_array.c \
	pair.c \
	string.c \
	topic_database.c \
	util.c \

SRCS += $(addprefix $(CURRENT_N2KAFKA_DIR), $(THIS_SRCS))

THIS_SRCS :=
