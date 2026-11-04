# JuiceFS 首次运行失败记录

2026-11-04（Asia/Shanghai），Bowl `0ab26a7`。`fio direct_randwrite_4jobs` 返回退出码 4，JSON 中的错误码为 5，完成写入 679,477,248 字节。服务端日志先记录 PostgreSQL 以 `SQLSTATE 57P01` 终止已有连接，随后连接本机 PostgreSQL 被拒绝；本次未采集数据库和内核日志，无法确定数据库停止的原因。

两台测试实例 `i-rj9ioglzn6ybnv6ylw3f`、`i-rj9ioglzn6ybnv6ylw3g` 已由清理脚本删除并核验目标实例数为 0；脚本随后调用了临时密钥对删除。压缩文件保留原始客户端和服务端日志，其他文件是失败前回传的原始测试输出。
