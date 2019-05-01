# ps
> ps [-aAcdefHjlmNVwy][acefghLnrsSTuvxX][-C <指令名称>][-g <群组名称>]
[-G <群组识别码>][-p <进程识别码>][p <进程识别码>][-s <阶段作业>]
[-t <终端机编号>][t <终端机编号>][-u <用户识别码>][-U <用户识别码>]
[U <用户名称>][-<进程识别码>][--cols <每列字符数>]
[--columns <每列字符数>][--cumulative][--deselect][--forest]
[--headers][--help][-- info][--lines <显示列数>][--no-headers]
[--group <群组名称>][-Group <群组识别码>][--pid <进程识别码>]
[--rows <显示列数>][--sid <阶段作业>][--tty <终端机编号>]
[--user <用户名称>][--User <用户识别码>][--version]
[--width <每列字符数>]

|            参数             |                                 解释                                 |
| -------------------------- | -------------------------------------------------------------------- |
| -a                         | 显示所有终端机下执行的进程，除了阶段作业领导者之外。                      |
| a                          | 显示现行终端机下的所有进程，包括其他用户的进程。                          |
| -A                         | 显示所有进程。                                                        |
| 　　-c                      | 显示CLS和PRI栏位。                                                    |
| 　　 c                      | 列出进程时，显示每个进程真正的指令名称，而不包含路径，参数或常驻服务的标示。 |
| 　　-C<指令名称>             | 　指定执行指令的名称，并列出该指令的进程的状况。                          |
| 　　-d                      | 　显示所有进程，但不包括阶段作业领导者的进程。                            |
| 　-e 　                     | 此参数的效果和指定"A"参数相同。                                         |
| 　　 e 　                   | 列出进程时，显示每个进程所使用的环境变量。                               |
| 　　-f                      | 　显示UID,PPIP,C与STIME栏位。                                         |
| 　　 f 　                   | 用ASCII字符显示树状结构，表达进程间的相互关系。                          |
| 　　-g<群组名称>             | 　此参数的效果和指定"-G"参数相同，当亦能使用阶段作业领导者的名称来指定。    |
| 　　 g                      | 　显示现行终端机下的所有进程，包括群组领导者的进程。                      |
| 　　-G<群组识别码> 　        | 列出属于该群组的进程的状况，也可使用群组名称来指定。                      |
| 　　 h                      | 　不显示标题列。                                                      |
| 　　-H 　                   | 显示树状结构，表示进程间的相互关系。                                     |
| 　　-j或j                   | 　采用工作控制的格式显示进程状况。                                      |
| 　　-l或l 　                | 采用详细的格式来显示进程状况。                                          |
| 　　 L 　                   | 列出栏位的相关信息。                                                   |
| 　　-m或m                   | 　显示所有的执行绪。                                                   |
| 　　 n                      | 　以数字来表示USER和WCHAN栏位。                                        |
| 　　-N 　                   | 显示所有的进程，除了执行ps指令终端机下的进程之外。                        |
| 　　-p<进程识别码>           | 　指定进程识别码，并列出该进程的状况。                                   |
| 　 　p<进程识别码>           | 　此参数的效果和指定"-p"参数相同，只在列表格式方面稍有差异。               |
| 　　 r 　                   | 只列出现行终端机正在执行中的进程。                                      |
| 　　-s<阶段作业>             | 　指定阶段作业的进程识别码，并列出隶属该阶段作业的进程的状况。             |
| 　 　s 　                   | 采用进程信号的格式显示进程状况。                                        |
| 　　 S                      | 　列出进程时，包括已中断的子进程资料。                                   |
| 　　-t<终端机编号>           | 　指定终端机编号，并列出属于该终端机的进程的状况。                        |
| 　　 t<终端机编号>           | 　此参数的效果和指定"-t"参数相同，只在列表格式方面稍有差异。               |
| 　　-T 　                   | 显示现行终端机下的所有进程。                                            |
| 　　-u<用户识别码>           | 　此参数的效果和指定"-U"参数相同。                                      |
| 　　 u                      | 　以用户为主的格式来显示进程状况。                                      |
| 　　-U<用户识别码>           | 　列出属于该用户的进程的状况，也可使用用户名称来指定。                     |
| 　　 U<用户名称>             | 　列出属于该用户的进程的状况。                                          |
| 　　 v 　                   | 采用虚拟内存的格式显示进程状况。                                        |
| 　　-V或V 　                | 显示版本信息。                                                        |
| 　　-w或w                   | 　采用宽阔的格式来显示进程状况。　                                      |
| 　 　x                      | 　显示所有进程，不以终端机来区分。                                      |
| 　　 X                      | 　采用旧式的Linux i386登陆格式显示进程状况。                            |
| 　　 -y                     | 配合参数"-l"使用时，不显示F(flag)栏位，并以RSS栏位取代ADDR栏位           |
| 　　-<进程识别码>            | 　此参数的效果和指定"p"参数相同。                                       |
| 　　--cols<每列字符数> 　    | 设置每列的最大字符数。                                                 |
| 　　--columns<每列字符数> 　 | 此参数的效果和指定"--cols"参数相同。                                    |
| 　　--cumulative            | 　此参数的效果和指定"S"参数相同。                                       |
| 　　--deselect              | 　此参数的效果和指定"-N"参数相同。                                      |
| 　　--forest                | 　此参数的效果和指定"f"参数相同。                                       |
| 　　--headers               | 　重复显示标题列。                                                     |
| 　　--help                  | 　在线帮助。                                                          |
| 　　--info                  | 　显示排错信息。                                                      |
| 　　--lines<显示列数>        | 设置显示画面的列数。                                                   |
| 　　--no-headers            | 此参数的效果和指定"h"参数相同，只在列表格式方面稍有差异。                  |
| 　　--group<群组名称>       | 　此参数的效果和指定"-G"参数相同。                                      |
| 　　--Group<群组识别码>     | 　此参数的效果和指定"-G"参数相同。                                      |
| 　　--pid<进程识别码>        | 　此参数的效果和指定"-p"参数相同。                                      |
| 　　--rows<显示列数>        | 　此参数的效果和指定"--lines"参数相同。                                 |
| 　　--sid<阶段作业>          | 　此参数的效果和指定"-s"参数相同。                                      |
| 　　--tty<终端机编号>        | 　此参数的效果和指定"-t"参数相同。                                      |
| 　　--user<用户名称>         | 　此参数的效果和指定"-U"参数相同。                                      |
| 　　--User<用户识别码>       | 　此参数的效果和指定"-U"参数相同。                                      |
| 　　--version 　            | 此参数的效果和指定"-V"参数相同。                                        |
| 　　--widty<每列字符数>      | 　此参数的效果和指定"-cols"参数相同。                                   |

## 举个例子
|                         命令                         |                解释                |
| ---------------------------------------------------- | ---------------------------------- |
| ps -e                                                | #显示所有进程                       |
| ps aux                                               | #不区分终端，显示所有用户的所有进程    |
| ps -ef                                               | #显示所有进程的UID,PPIP,C与STIME栏位 |
| ps -u zhangy                                         | #显示zhangy用户的所有进程            |
| ps axo pid,comm,pcpu                                 | //查看进程的PID、名称以及CPU 占用率   |
| ps axo pid,comm,pcpu --sort=pcpu                     | // sort 参数以pcpu 为对象对         |
| ps -ef \| grep rpc.rstatd                            | #查找rpc.rstatd进程                 |
| ps -efL                                              | #查看线程数                         |
| ps -e -o "%C : %p :%z : %a"\|sort -k5 -nr            | #查看进程并按内存使用大小排列         |
| ps -C nginx                                          | #通过名字或命令搜索进程              |
| ps aux --sort=-pcpu,+pmem                            | #CPU或者内存进行排序,-降序，+升序     |
| ps -f --forest -C nginx                              | #用树的风格显示进程的层次关系         |
| ps -o pid,uname,comm -C nginx                        | #显示一个父进程的子进程              |
| ps --ppid 32633                                      | #根据ppid显示                       |
| ps -e -o pid,uname=USERNAME,pcpu=CPU_USAGE,pmem,comm | #重定义标签                         |
| ps -e -o pid,comm,etime                              | #显示进程运行的时间                  |
| ps -aux \| grep named                                | #查看named进程详细信息              |