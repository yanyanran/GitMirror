# 心跳事件类型
NO_MORE = 0
ADD_DEL_REPO = 1

# 判断网络连接问题 True-1  False-0
def std_neterr(stderr):
    err = 'failed: The TLS connection was non-properly terminated.'
    err2 = 'Could not resolve host: gitee.com'
    err3 = 'failed: Error in the pull function.'
    if err in stderr :
        return 1
    elif err2 in stderr:
        return 1
    elif err3 in stderr:
        return 1
    else:
        return 0

# clone-2 ; neterr-1 ; other-0
def std_cloneerr(stderr):
    err = 'fatal'
    err2 = 'already exists and is not an empty directory.'
    err21 = '已经存在，并且不是一个空目录。'
    err3 = 'Could not read from remote repository.'
    if err in stderr:
        if err2 in stderr or err21 in stderr:
            return 2     # is cloned
        elif err3 in stderr:
            return 1     # 仓库不存在
    return 0

def std_fetcherr(stderr):
    err = 'fatal: not a git repository' # 仓库不存在
    if err in stderr:
        return 1
    return 0