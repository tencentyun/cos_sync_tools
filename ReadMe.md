## 已弃用 - 请升级到最新版 cos_sync_tools

> 工具已弃用，请直接使用基于 XML API 的 [cos_sync_tools_v5](https://github.com/tencentyun/cos_sync_tools_v5)。

同步工具用于将本地的文件和目录上传到cos上, 文件和目录结构和本地一致
此同步工具用户cos 4.x

# COS版本
4.X

# 系统要求
jdk 1.7或1.8 

# 配置方法
1. 配置conf/config.json里的appid, secret_id, secret_key, local_path
   可在cos控制台(https://console.qcloud.com/cos)查询
2. 配置conf/config.json中要同步的目录路径local_path
3. 配置cos 4.x 的bucket分区region, 目前有sh(华东上海), gz(华南广州), tj(华北天津)等区域, 更多的园区在上线中, 可在控制台查看region

# 运行

linux环境下
sh start_cos_sync.sh

windows环境下
双击 start_cos_sync.bat


# 目录信息
conf : 配置文件目录
log  : 日志目录
db   : 存储同步记录的数据库文件目录
src  : java 源程序
dep  : 编译生成的可运行的JAR包

# 常见问题
1 同步工具只针对本地发生的变更进行操作, 比如同步了a.txt到cos上,但如果用户通过控制台删除了a.txt, 则再次运行同步工具，不会把本地的a.txt上传上去, 所以不要绕过cos同步工具在别的地方修改要同步的文件
2 同步工具记录的数据保存在db目录下的数据文件里，如果删除该文件, 再运行同步工具会对数据目录下的文件全量的上传一遍, 如果cos上存在该文件, 则会出错并提示文件已存在
3 上传前确保已经在控制台上创建过bucket
4 windows路径使用\\进行分割, 因为如果使用\, 配置文件中某些特殊字符会被当做被转义, 整个文件不是一个有效的json
5 可以通过增大thread_num的数量提高并发量，达到增大上传和删除的速度
6 如果有偶发的失败问题，可以重跑同步程序
