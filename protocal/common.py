# 心跳事件类型
NO_MORE = 0
ADD_DEL_REPO = 1

# 判断网络连接问题
def std_neterr(stderr):
    err = 'failed: The TLS connection was non-properly terminated.'
    err2 = 'Could not resolve host: gitee.com'
    err3 = 'failed: Error in the pull function.'
    if err in stderr :
        return True
    elif err2 in stderr:
        return True
    elif err3 in stderr:
        return True
    else:
        return False