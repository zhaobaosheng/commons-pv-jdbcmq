--表名可以自定义,其需要在JdbcMessageQueue类配置,字段不能改变.
CREATE TABLE msg_queue (
  id varchar(40) COMMENT '消息标识',
  content blob COMMENT '消息内容',
  create_time datetime COMMENT '消息创建时间',
  exec_finishtime datetime COMMENT '消息执行时间',
  exec_count decimal(2) COMMENT '执行次数',
  msg_state decimal(1) COMMENT '消息状态 0未执行 1执行中 2执行失败 3错误消息',
  PRIMARY KEY(id)
);