# -*- coding: UTF-8 -*-
import os
import zipfile
import sys

# import git

"""
   pip install GitPython
"""


def err_exit(msg):
    if msg: print('%s' % msg)
    sys.exit(0)


# 打包目录为zip文件（未压缩）
def make_zip(source_dir, output_filename):
    zip_file = zipfile.ZipFile(output_filename, 'w')
    pre_len = len(os.path.dirname(source_dir))
    for path_file in get_files(source_dir):
        arc_name = path_file[pre_len:].strip(os.path.sep)  # 相对路径
        zip_file.write(path_file, arc_name)
    zip_file.close()


def dir_last_name(source_dir):
    pre_len = len(os.path.dirname(source_dir))
    return source_dir[pre_len:].strip("/")


def get_files(root):
    for parent, dir_names, filenames in os.walk(root):
        for filename in filenames:
            yield os.path.join(parent, filename).replace("\\", "/")


def format_file(file, to_format='unix2dos', file_types=None):
    if file_types is None:
        file_types = ['sh']
    if os.path.splitext(file)[1].strip(".").lower() not in file_types:
        print('ERROR: %s invalid file type ' % file)
        return
    print('Formatting %s:\t%s' % (to_format, file))
    if not os.path.isfile(file):
        print('INFO: %s invalid normal file' % file)
        return
    if to_format == 'unix2dos':
        line_sep = b'\r\n'
    else:
        line_sep = b'\n'
    with open(file, 'r+b') as fd:
        tmp_file = open(file + to_format, 'w+b')
        for line in fd:
            line = line.replace(b'\r', b'')
            line = line.replace(b'\n', b'')
            tmp_file.write((line + line_sep))
        fd.close()
        tmp_file.close()
        try:
            os.rename(file + to_format, file)
        except WindowsError:
            os.remove(file)
            os.rename(file + to_format, file)


def unix2dos(filename, file_types=None):
    if not filename or not os.path.exists(filename):
        err_exit('ERROR: %s: No such file or directory' % filename)
    if os.path.isfile(filename):
        format_file(filename, 'unix2dos', file_types)
        return
    if os.path.isdir(filename):
        for file in get_files(filename, ):
            unix2dos(file, file_types)


def dos2unix(filename, file_types=None):
    if not filename or not os.path.exists(filename):
        err_exit('ERROR: %s: No such file or directory' % filename)
    if os.path.isfile(filename):
        format_file(filename, 'dos2unix', file_types)
        return
    if os.path.isdir(filename):
        for file in get_files(filename):
            dos2unix(file, file_types)


def file_line_replace_text(file, src_text, replace_text):
    """
     替换文件中某些文本，按行替换的
    :param file:
    :param src_text:
    :param replace_text:
    :return:
    """
    if os.path.isfile(file) is not True:
        err_exit("ERROR: %s: No a file" % file)
    file_data = ""
    with open(file, "r", encoding='utf-8') as fd:
        for line in fd:
            file_data += line.replace(src_text, replace_text)
    with open(file, "w", encoding="utf-8") as f:
        f.write(file_data)

# def git_tag(repo_dir, name, message=None):
#     repo = git.Repo.init(repo_dir)
#     # tag_str = " -a %s -m \"%s\"" % (name, message)
#     repo.git.tag().create(name, message=message)
#     repo.git.push()
#
#
# if __name__ == '__main__':
#     print(git_tag("./", "test_tag_0.1", "测试打标签"))
