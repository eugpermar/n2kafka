THIS_SRCS := \
	in_addr_list.c \
	kafka.c \
	kafka_message_list.c \
	pair.c \
	rb_json.c \
	rb_mac.c \
	topic_database.c \
	rb_timer.c \
	tommyds/tommyhash.c \
	tommyds/tommyhashdyn.c \
	tommyds/tommylist.c \

SRCS += $(addprefix $(CURRENT_N2KAFKA_DIR), $(THIS_SRCS))

THIS_SRCS :=
