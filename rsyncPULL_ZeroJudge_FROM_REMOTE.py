import os, sys
import subprocess
import platform
import fire, socket
import datetime

"""
使用方式 SLAVE2 備援方式:
* 兩台主機進行定時備援(非即時備援，即時備援要使用 mysql replication)。
* REMOTE 為主要主機。 LOCAL 為備援主機
* 自動建立 SSH 免密碼連線
* LOCAL 向 REMOTE 定時 
STEP1. 直接在遠端 dump 資料庫
STEP2. rsync 
STEP3. restore
"""


def localCmd(localcmd, comment=""):
    """
    本地端執行指令
    """
    print(comment, localcmd)
    start = datetime.datetime.now()
    print(f"BEGIN [{start}]: 執行本地指令 = " + localcmd)
    os.system(localcmd)
    print(f"END: 執行:[{localcmd}] 共花費: {datetime.datetime.now()-start} 秒")


def remoteCmd(remotecmd, rasfile, MASTER_account, MASTER_host, comment=""):
    """
    組合出遠端命令
    ssh -i {rasfile} {NAS_account}@{NAS_ip} "rsync -av --delete {NAS_path}/ {NAS_path}_{nowstr}"
    """
    cmd = f'ssh -i {rasfile} {MASTER_account}@{MASTER_host} "{remotecmd}"'
    print("執行遠端指令：", comment, cmd)
    start = datetime.datetime.now()
    print(f"BEGIN [{start}]: 執行遠端指令 = " + cmd)
    os.system(cmd)
    print(f"END: 執行:[{cmd}] 共花費: {datetime.datetime.now()-start} 秒")


def run(cmd, printout=True):
    if printout:
        print("執行指令=", cmd)
    try:
        completed = subprocess.run(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True
        )
    except subprocess.CalledProcessError as err:
        print(f"subprocess.CalledProcessError:cmd={err.cmd}")
        print(f"subprocess.CalledProcessError:output={err.output}")
        print(f"subprocess.CalledProcessError:stderr={err.stderr}")
        msg = f"cmd={err.cmd}\n"
        msg += f"output={err.output}\n"
        msg += f"stderr={err.stderr}\n"
    else:
        STDOUT = completed.stdout.decode("utf-8").strip()
        STDERR = completed.stderr.decode("utf-8").strip()
        if printout:
            print(f"(return:{completed.returncode}) CMD:{cmd}")
            print(f"(len:{len(completed.stdout)}) STDOUT: {STDOUT}")
            print(f"(len:{len(completed.stderr)}) STDERR: {STDERR}")
        return completed.returncode, STDOUT, STDERR


def getIP():
    myname = socket.getfqdn(socket.gethostname())
    get_s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    get_s.connect(("8.8.8.8", 0))
    ip = ("hostname: %s, localIP: %s") % (myname, get_s.getsockname()[0])
    return get_s.getsockname()[0]


def get_tomcatGroup(LOCAL_account):
    tomcatGroup = "tomcat?"
    returncode, STDOUT, STDERR = run(f"sudo -u {LOCAL_account} groups")
    for s in STDOUT.split():
        if "tomcat" in s:
            tomcatGroup = s
    return tomcatGroup


def do_NoPassLogin(REMOTE_account, REMOTE_host, LOCAL_home, LOCAL_account):
    """
    處理 免密碼登入
    """
    print(f"用於從 {REMOTE_host} 直接同步資料回來，目前本機IP為:{getIP()}。")
    print(f"{REMOTE_host} 的登入方式為： ssh {REMOTE_account}@{REMOTE_host}")
    # print(
    #     f"{REMOTE_host} 的 rsync 指令: #rsync -av -e ssh {REMOTE_account}@{REMOTE_host}:{REMOTE_dumppath}/*.sql {LOCAL_home}"
    # )
    rasfile = f"{LOCAL_home}/id_rsa_{getIP()}_TO_{REMOTE_host}"

    print("======================")
    print(f"1. 產生key: ssh-keygen -t rsa -f {rasfile}")
    print(f"2. 複製到遠端：ssh-copy-id -i {rasfile} {REMOTE_account}@{REMOTE_host}")
    print(f"3. 測試：ssh -i {rasfile} {REMOTE_account}@{REMOTE_host}")
    print("======================")

    if getIP() == "163.32.92.12":
        print(f"不能 PULL 到官網。{getIP()}")
        sys.exit()

    if not os.path.isfile(rasfile):
        print("!!! 免密碼登入檔案不存在, 重新產生 !!!")
        localCmd(f"sudo -u {LOCAL_account} ssh-keygen -t rsa -f {rasfile}")
        localCmd(
            f"sudo -u {LOCAL_account} ssh-copy-id -i {rasfile} {REMOTE_account}@{REMOTE_host}"
        )
        localCmd(
            f"sudo -u {LOCAL_account} ssh -i {rasfile} {REMOTE_account}@{REMOTE_host}"
        )
    return rasfile


def do_rsyncFiles(
    rasfile, REMOTE_account, REMOTE_host, LOCAL_account, tomcatGroup, REMOTE_consolepath
):
    """
    1. 將遠端的 檔案資料(CONSOLE) 同步回來
    2. 在 LOCAL 端更改 owner
    """
    ##
    localCmd(
        f'rsync -av --delete --progress --chmod=D770,F660 -e "ssh -i {rasfile}" {REMOTE_account}@{REMOTE_host}:{REMOTE_consolepath} /'
    )
    localCmd(f"chown -R {LOCAL_account}:{tomcatGroup} {REMOTE_consolepath}/")


def do_dumpDataBase(
    REMOTE_dbname,
    REMOTE_doDump,
    REMOTE_dbpass,
    rasfile,
    REMOTE_account,
    REMOTE_host,
    REMOTE_dumppath,
    LOCAL_home,
):
    """
    1. 遠端: 一次將整個 database dump 下來。
    2. 遠端: 將已經 dump 出來的 .sql 同步回來。
    """
    dumpfile_dbname = f"dump_{REMOTE_dbname}.sql"
    if REMOTE_doDump:
        remote_cmd = f"export MYSQL_PWD='{REMOTE_dbpass}';"
        remote_cmd += f"mysqldump -uroot --compress --quick --triggers --routines --lock-tables=false --single-transaction {REMOTE_dbname} > {REMOTE_dumppath}/{dumpfile_dbname}.BAK"
        remoteCmd(remote_cmd, rasfile, REMOTE_account, REMOTE_host, "dump 遠端資料庫")
        remoteCmd(
            f"rm {REMOTE_dumppath}/{dumpfile_dbname}",
            rasfile,
            REMOTE_account,
            REMOTE_host,
            "先刪除 舊的 dump 資料庫",
        )
        remoteCmd(
            f"mv {REMOTE_dumppath}/{dumpfile_dbname}.BAK {dumpfile_dbname}",
            rasfile,
            REMOTE_account,
            REMOTE_host,
            "mv BAK 為正式 sql",
        )
        remoteCmd(
            f"touch {REMOTE_dumppath}/{dumpfile_dbname}",
            rasfile,
            REMOTE_account,
            REMOTE_host,
            "touch",
        )

    ## 把遠端已經 dump 出來的 .sql 同步回來。
    localCmd(
        f'rsync -av --delete --progress --exclude "*.BAK" -e "ssh -i {rasfile}" {REMOTE_account}@{REMOTE_host}:{REMOTE_dumppath}/{dumpfile_dbname} {LOCAL_home}'
    )
    return dumpfile_dbname


def do_DataBaseRestore(LOCAL_dbpass, LOCAL_dbname, LOCAL_home, dumpfile_dbname):
    """
    用 .sql 檔還原 DataBase
    """
    localCmd(
        f"export MYSQL_PWD='{LOCAL_dbpass}'; mysql -uroot {LOCAL_dbname} < {LOCAL_home}/{dumpfile_dbname}"
    )


def 同步_Database(
    LOCAL_dbpass="!@34ZeroJudge",
    REMOTE_dbpass="!@34ZeroJudge",
    LOCAL_account="zero",
    LOCAL_home="/home/zero",
    LOCAL_dbname="zerojudge",  # 還原回本地端的資料庫名稱
    REMOTE_doDump=True,  # 同步時是否在遠端進行 dump? 遠端已有 dump 完成的 sql。
    REMOTE_dbname="zerojudge",  # 要 dump 的資料庫名稱
    REMOTE_ignoretable=None,  # 預設不需要 ignoretable
    REMOTE_account="zero",
    REMOTE_host="slave1.zerojudge.tw",  # REMOTE 典型 slave1 盡量不從正式機同步
    REMOTE_dumppath="/home/zero",  # 遠端 dump 下來的 sql 放置路徑
    REMOTE_consolepath="/ZeroJudge_CONSOLE",  # 遠端的 CONSOLE path
):
    """
    REMOTE 為 NAS 時。參數如下:
    REMOTE_account="jiangsir",
    REMOTE_host="163.32.92.26",
    REMOTE_dumppath="/mnt/d/zerojudge.tw_data/mysql",
    REMOTE_consolepath="/mnt/d/zerojudge.tw_data/ZeroJudge_CONSOLE",
    """

    rasfile = do_NoPassLogin(REMOTE_account, REMOTE_host, LOCAL_home, LOCAL_account)

    tomcatGroup = get_tomcatGroup(LOCAL_account)
    
    # 在遠端 dump 資料庫，並同步回來。
    dumpfile_dbname = do_dumpDataBase(
        REMOTE_dbname,
        REMOTE_doDump,
        REMOTE_dbpass,
        rasfile,
        REMOTE_account,
        REMOTE_host,
        REMOTE_dumppath,
        LOCAL_home,
    )

    # 還原資料庫
    do_DataBaseRestore(LOCAL_dbpass, LOCAL_dbname, LOCAL_home, dumpfile_dbname)

    localCmd(f"sudo systemctl restart mysql")
    localCmd(f"sudo systemctl restart {tomcatGroup}")

def 同步_CONSOLE(
    LOCAL_account="zero",
    LOCAL_home="/home/zero",
    REMOTE_account="zero",
    REMOTE_host="slave1.zerojudge.tw",  # REMOTE 典型 slave1 盡量不從正式機同步
    REMOTE_consolepath="/ZeroJudge_CONSOLE",  # 遠端的 CONSOLE path
):
    rasfile = do_NoPassLogin(REMOTE_account, REMOTE_host, LOCAL_home, LOCAL_account)

    tomcatGroup = get_tomcatGroup(LOCAL_account)

    # 同步 CONSOLE 資料回來。
    do_rsyncFiles(
        rasfile,
        REMOTE_account,
        REMOTE_host,
        LOCAL_account,
        tomcatGroup,
        REMOTE_consolepath,
    )

    localCmd(f"sudo systemctl restart {tomcatGroup}")
    
if __name__ == "__main__":
    fire.Fire()
