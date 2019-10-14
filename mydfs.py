'''
@Description: In User Settings Edit
@Author: your name
@Date: 2019-09-17 12:07:54
@LastEditTime: 2019-09-22 21:27:46
@LastEditors: Please set LastEditors
'''
from pyhdfs import HdfsClient, HdfsException
import argparse as ap
import os
import urllib3 as url3
import time,datetime

parser = ap.ArgumentParser()
group = parser.add_mutually_exclusive_group()

group.add_argument('-u', help="上传本地文件到HDFS。", action='store_true')

group.add_argument('-d', help="从HDFS下载文件到本地。", action='store_true')
group.add_argument('-s', help="输出HDFS文件内容到shell。", action='store_true')
group.add_argument('-i', help="输出文件或目录的详细信息。", action='store_true')
group.add_argument('-r', help="删除文件或目录。", action='store_true')
group.add_argument('-c', help="创建文件。", action='store_true')
group.add_argument('-m', help="移动文件或重命名。", action='store_true')

parser.add_argument('files', help="get some inputed files", nargs='+')

group__ = parser.add_mutually_exclusive_group()
group__.add_argument('-f', help='和-r搭配，强制删除非空文件夹。', action='store_true')
group__.add_argument('-o', help='和-u搭配，若文件已存在则覆盖。', action='store_true')
group__.add_argument('-ah', help='和-u搭配，若文件已存在则追加在文件头。', action='store_true')
group__.add_argument('-at', help='和-u搭配，若文件已存在则追加在文件尾部。', action='store_true')
args = parser.parse_args()

try:
    client = HdfsClient(hosts="localhost")
except HdfsException as he:
    print(str(he).split('\n')[0])


def upload(local: str, dest: str):
    """
    向HDFS中上传任意文本文件，如果指定的文件已在HDFS存在，
    由用户选择追加还是覆盖
    """
    try:
        # 覆盖原文件
        if args.u and args.ah==False and args.at==False and args.o==False:
            client.copy_from_local(local, dest)
            return
        if args.u and args.o:
            client.copy_from_local(local, dest, overwrite=True)
            return
        # 文件已存在采取追加方式上传
        http = client.open(dest)
        file1 = http.data.decode()
        with open(local) as f:
            file2 = f.read()
        # 追加在文件头
        if args.u and args.ah:
            file1 = file2+'\n'+file1
            client.create(dest, str.encode(file1),overwrite=True)
        # 追加在文件尾部
        elif args.u and args.at:
            file2 = '\n'+file2
            client.append(dest, str.encode(file2))
    except HdfsException as he:
        print(str(he).split('\n')[0])


def get_filename(path: str):
    """
    返回路径和文件名
    """
    index = path.rfind('/')
    return path[:index+1], path[index+1:]


def numOfback(filename: str) -> int:
    """
    读取文件夹所有文件信息，返回副本号
    """
    # 返回文件的路径和文件名
    path, file_name = get_filename(filename)
    # print('filename = ',file_name)
    lens = len(file_name)
    index1 = file_name.rfind('.')
    len_name = file_name[:index1]
    max_ = 0
    for name in os.listdir(path):
        if index1 > 0:
            if name[:index1] == len_name:
                if len(name) == lens and max_ == 0:
                    max_ = 0
                else:
                    if name[index1] == '(':
                        i = name.rfind(')')
                        max_ = int(
                            name[index1+1:i]) if int(name[index1+1:i]) > max_ else max_
        else:
            if name[-1] == ')':
                left = name.rfind('(')
                max_ = int(name[left+1:-1]
                           ) if int(name[left+1:-1]) > max_ else max_
    return max_+1


def download(hdfspath: str, local: str):
    """
    从HDFS下载指定文件，如本地已存在，则对下载的文件编号
    """
    try:
        if os.path.exists(local):
            n = numOfback(local)
            index = local.rfind('.')
            _, filename = get_filename(local)
            # print(filename)
            tmp = ""
            if filename[0] != '.':
                tmp = local[:index] + \
                    "(%d)" % n+local[index:]
            else:
                tmp = local+"(%d)" % n
            client.copy_to_local(hdfspath, tmp)
        else:
            client.copy_to_local(hdfspath, local)
    except HdfsException as he:
        print(str(he).split('\n')[0])


def show_to_shell(path: str):
    """
    将HDFS文件的内容输出到终端
    """
    try:
        http = client.open(path)
        print(http.data.decode())
    except HdfsException as he:
        print(str(he).split('\n')[0])

def num_to_chmod(num:int):
    dic={0:'---',1:'--x',2:'-w-',3:'-wx',4:'r--',5:'r-x',6:'rw-',7:'rwx'}
    strn = str(num)
    sums=''
    for i in strn:
        sums+=dic[int(i)]
    return sums

def statusFormat(status,path):
    ret = ''
    if status.type=='FILE':
        ret='-'+num_to_chmod(status.permission)
    else:
        ret ='d'+num_to_chmod(status.permission)
    formatstr="{0:<5}\t{1:<5}\t{2:<5}\t{3:<5}\t{4:<5}"
    date = time.localtime(int(str(status.modificationTime)[:10]))
    # print(date,int(str(status.modificationTime)[:10]))
    return formatstr.format(ret,status.owner,str(status.length),time.strftime("%Y-%m-%d %H:%M",date),path)

def getStatus(path: str):
    """
    获取文件详细信息
    """
    try:
        state = client.get_file_status(path)
        print(statusFormat(state,path))
    except HdfsException as he:
        print(str(he).split('\n')[0])


def walkFrom(path: str):
    """
    递归获取文件夹下所有文件和文件夹的信息
    """
    try:
        for root, dirs, files in client.walk(path):
            # print('root: ',root)
            # print('dirs:',dirs)
            # print('files: ',files)
            for dir_ in dirs:
                # print("dir : %s" % (root+dir_))
                string = root+dir_ if len(root) == 1 else root+'/'+dir_
                state = client.get_file_status(string)
                print(statusFormat(state,string))
            for file_ in files:
                string = root+file_ if len(root) == 1 else root+'/'+file_
                # print("file : %s" % (string))
                state = client.get_file_status(string)
                print(statusFormat(state,string))
    except HdfsException as he:
        print(str(he).split('\n')[0])


def remove(path: str, force=False):
    """
    删除指定HDFS文件或文件夹
    """
    try:
        if client.delete(path, recursive=force):
            print('Delete %s successfully!' % path)
        else:
            print('Delet %s failed!!!' % path)
    except HdfsException as he:
        print(str(he).split('\n')[0])


def create(path: str):
    """
    创建HDFS文件或文件夹
    """
    cur, name = get_filename(path)
    # print(cur, name)
    # 创建文件
    try:
        if cur and (not name):
            i=2
            while(i<len(cur)):
                idex=cur.find('/',i)
                print("create dir:",cur[:idex])
                if client.exists(cur[:idex]) != True:
                    client.mkdirs(cur[:idex])
                i =idex+1
        elif name and client.exists(cur):
            print('create file',path)
            client.create(path, str.encode(name))
    except HdfsException as he:
        print(str(he).split('\n')[0])


def moveFile(start: str, end: str):
    """
    将HDFS上的文件移动到另一个路径
    """
    # 获取文件内容
    try:
        http = client.open(start)
        content = http.data.decode()
        _, name = get_filename(start)
        end = end+'/'+name if end[-1] != '/' else end+name
        client.create(end, str.encode(content))
        client.delete(start)
    except HdfsException as he:
        print(str(he).split('\n')[0])


def main():
    # 上传
    if args.u:
        if len(args.files) == 2:
            upload(args.files[0], args.files[1])
        else:
            print("Error: Argument requires 2 but get %d!!!" % len(args.files))

    # 下载
    elif args.d:
        if len(args.files) == 2:
            download(args.files[0], args.files[1])
        else:
            print("Error: Argument requires 2 but get %d!!!" % len(args.files))

    # 输出内容到终端
    elif args.s:
        if len(args.files) == 1:
            show_to_shell(args.files[0])
        else:
            print("Error: Argument requires 1 but get %d!!!" % len(args.files))

    # 输出详细信息
    elif args.i:
        if len(args.files) == 1:
            try:
                state = client.get_file_status(args.files[0])
                if state['type'] == "DIRECTORY":
                    walkFrom(args.files[0])
                else:
                    getStatus(args.files[0])
            except HdfsException as he:
                print(he)
        else:
            print("Error: Argument requires 1 but get %d!!!" % len(args.files))

    # 删除文件或目录
    elif args.r :
        if len(args.files) == 1:
            remove(args.files[0], args.f)
        else:
            print("Error: Argument requires 1 but get %d!!!" % len(args.files))

    # 创建文件或目录
    elif args.c :
        if len(args.files) == 1:
            create(args.files[0])
        else:
            print("Error: Argument requires 1 but get %d!!!" % len(args.files))

    # 移动文件
    elif args.m:
        if len(args.files) == 2:
            moveFile(args.files[0], args.files[1])
        else:
            print("Error: Argument requires 2 but get %d!!!" % len(args.files))


main()
