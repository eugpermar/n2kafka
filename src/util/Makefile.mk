THIS_SRCS := \
	in_addr_list.c \
	kafka.c \
	kafka_message_array.c \
	pair.c \
	string.c \
	rb_json.c \
	rb_mac.c \
	topic_database.c \
	rb_timer.c \

SRCS += $(addprefix $(CURRENT_N2KAFKA_DIR), $(THIS_SRCS))

THIS_SRCS :=
