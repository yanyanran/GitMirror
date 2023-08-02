# SPDX-License-Identifier: MulanPSL-2.0+
# Copyright (c) 2020 Huawei Technologies Co., Ltd. All rights reserved.
# frozen_string_literal: true

require 'yaml'
require 'fileutils'
require 'bunny'
require 'json'
# gem install PriorityQueue
require 'priority_queue'
require 'English'
require 'elasticsearch'
require_relative 'constants.rb'
require_relative 'json_logger.rb'
require 'erb'

# worker threads
class GitMirror
  ERR_MESSAGE = <<~MESSAGE # 错误信息多行字符串
    fatal: not a git repository (or any parent up to mount point /srv)
    Stopping at filesystem boundary (GIT_DISCOVERY_ACROSS_FILESYSTEM not set).
  MESSAGE
  ERR_CODE = 128

  def initialize(queue, feedback_queue) # 类的构造函数
    @queue = queue
    @feedback_queue = feedback_queue
    @feedback_info = {}
  end

  def feedback(git_repo, possible_new_refs, last_commit_time)
    # 存哈希
    @feedback_info = { git_repo: git_repo, possible_new_refs: possible_new_refs, last_commit_time: last_commit_time }
    # 推送到实例变量@feedback_queue所表示的队列中
    @feedback_queue.push(@feedback_info)
  end

  def get_url(url)
    # 如果URL包含'gitee.com/'字符串，并且在文件系统上存在以"/srv/git/"开头的目录路径
    if url.include?('gitee.com/') && File.exist?("/srv/git/#{url.delete_prefix('https://')}")
      url = "/srv/git/#{url.delete_prefix('https://')}" # 将URL更新为对应的目录路径
    end
    return url
  end

  def stderr_443?(stderr_info) # 用于检查stderr_info是否包含特定的错误信息
    return true if stderr_info.include?('Failed to connect to github.com port 443')

    return false
  end

  def git_clone(url, mirror_dir)
    url = get_url(Array(url)[0])
    10.times do
      stderr = %x(git clone --mirror --depth 1 #{url} #{mirror_dir} 2>&1)
      # mirror_dir已存在且包含config文件（克隆成功）-> ret 2
      return 2 if File.directory?(mirror_dir) && File.exist?("#{mirror_dir}/config")

      # 如果出现连接错误(port 443错误)，会尝试更新URL的协议为"git://"，最多尝试10次
      url = "git://#{url.split('://')[1]}" if stderr_443?(stderr)
    end
    return -2
  end

  # 如果fetch_info包含特定的错误信息，表示发生了443错误，则获取仓库的URL并尝试将其协议更新为"git://"以解决连接问题。
  def fetch_443(mirror_dir, fetch_info)
    return unless stderr_443?(fetch_info)

    url = %x(git -C #{mirror_dir} ls-remote --get-url origin).chomp
    %x(git -C #{mirror_dir} remote set-url origin git://#{url.split('://')[1]})
  end

  # 更新git仓库
  def git_fetch(mirror_dir)
    # 检查mirror_dir目录是否包含"shallow"文件
    if File.exist?("#{mirror_dir}/shallow")
      # 有，则使用git fetch --unshallow命令将仓库变成完整仓库，然后使用git fetch命令从远程仓库获取更新。
      FileUtils.rm("#{mirror_dir}/shallow.lock") if File.exist?("#{mirror_dir}/shallow.lock")
      %x(git -C #{mirror_dir} fetch --unshallow 2>&1)
    end

    fetch_info = %x(git -C #{mirror_dir} fetch 2>&1)
    # Check whether mirror_dir is a good git repository by 2 conditions. If not, delete it.
    # 检查fetch_info中是否包含了特定的错误信息(ERR_MESSAGE)，以及mirror_dir是否为空目录
    if fetch_info.include?(ERR_MESSAGE) && Dir.empty?(mirror_dir)
      FileUtils.rmdir(mirror_dir) # 删除该目录
    end
    fetch_443(mirror_dir, fetch_info)
    # 根据fetch_info中的输出结果判断是否成功，返回相应的值（1表示有新的提交，0表示没有新的提交，-1表示发生了致命错误）。
    return -1 if fetch_info.include?('fatal')

    return fetch_info.include?('->') ? 1 : 0
  end

  # 检查给定URL与Git仓库在mirror_dir中保存的URL是否相同
  def url_changed?(url, mirror_dir)
    url = get_url(Array(url)[0])
    # 使用git ls-remote --get-url origin命令获取mirror_dir仓库的远程URL
    git_url = %x(git -C #{mirror_dir} ls-remote --get-url origin).chomp

    # 比较两个URL是否相同或者是否包含相同的URL主机部分-> true改变
    return true if git_url == url || git_url.include?(url.split('://')[1])

    return false
  end

  # 下载Git仓库
  def git_repo_download(url, mirror_dir)
    # 检查mirror_dir是否已存在
    return git_clone(url, mirror_dir) unless File.directory?(mirror_dir) # 不存在则调用git_clone方法进行克隆
    # 已存在，检查URL是否发生了变化
    return git_fetch(mirror_dir) if url_changed?(url, mirror_dir) # 没变化，直接调用git_fetch方法进行更新

    # 发生了变化，先删除旧的mirror_dir，再调用git_clone方法进行克隆
    FileUtils.rm_r(mirror_dir)
    return git_clone(url, mirror_dir)
  end

  # 执行仓库同步
  def mirror_sync
    fork_info = @queue.pop # 从队列中获取一个元素
    # 根据获取到的元素中的信息，构建mirror_dir路径，以及获取可能的新提交数和最后提交的时间
    mirror_dir = "/srv/git/#{fork_info['belong']}/#{fork_info['git_repo']}"
    mirror_dir = "#{mirror_dir}.git" unless fork_info['is_submodule']

    possible_new_refs = git_repo_download(fork_info['url'], mirror_dir) # 下载/更新
    last_commit_time = %x(git -C #{mirror_dir} log --pretty=format:"%ct" -1 2>/dev/null).to_i # 获取最后提交时间
    feedback(fork_info['git_repo'], possible_new_refs, last_commit_time) # 提供反馈信息
  end

  # 不断执行仓库同步
  def git_mirror
    loop do
      mirror_sync
    end
  end
end

# main thread
class MirrorMain
  REPO_DIR = ENV['REPO_SRC'] # 环境变量REPO_SRC

  def initialize   
    @feedback_queue = Queue.new # 队列存储反馈信息
    @fork_stat = {} # 存储仓库的状态信息
    @priority_queue = PriorityQueue.new # 优先队列，存储仓库的优先级信息
    @git_info = {} # 存储Git仓库信息
    @defaults = {} # 存储默认配置信息
    @git_queue = Queue.new # 存储Git仓库同步任务的队列
    @log = JSONLogger.new
    @es_client = Elasticsearch::Client.new(hosts: ES_HOSTS) # Elasticsearch客户端

    @git_mirror = GitMirror.new(@git_queue, @feedback_queue)
    # 初始化操作：克隆上游仓库、加载分叉信息、初始化连接、处理Webhook和PR Webhook
    clone_upstream_repo
    load_fork_info
    connection_init
    handle_webhook
    handle_pr_webhook
  end

  # 初始化连接
  def connection_init
    connection = Bunny.new('amqp://172.17.0.1:5672') # 创建一个RabbitMQ连接
    connection.start
    channel = connection.create_channel
    @message_queue = channel.queue('new_refs') # 存储新的提交信息
    @webhook_queue = channel.queue('web_hook') # 存储Webhook信息
    @webhook_pr_queue = connection.create_channel.queue('openeuler-pr-webhook') # 存储PR Webhook信息
  end

  # 初始化特定git_repo的分叉状态信息
  def fork_stat_init(git_repo)
    @fork_stat[git_repo] = get_fork_stat(git_repo) # 调用get_fork_stat方法获取分叉信息，并将其存储在@fork_stat哈希中
  end

  # 加载默认配置信息
  def load_defaults(defaults_file, belong)
    return unless File.exist?(defaults_file)

    # 默认配置文件存在-> 加载配置信息，并将其存储在@defaults哈希中
    repodir = File.dirname(defaults_file)
    defaults_key = repodir == "#{REPO_DIR}/#{belong}" ? belong : repodir.delete_prefix("#{REPO_DIR}/#{belong}/")
    @defaults[defaults_key] = YAML.safe_load(File.open(defaults_file))
    @defaults[defaults_key] = merge_defaults(defaults_key, @defaults[defaults_key], belong)
  end

  # 遍历指定目录下的Git仓库文件
  def traverse_repodir(repodir, belong)
    # 在给定目录中查找名为"DEFAULTS"的文件，加载默认配置信息，并遍历除"DEFAULTS"外的所有文件，调用load_repo_file方法加载仓库文件信息
    defaults_list = %x(git -C #{repodir} ls-files | grep 'DEFAULTS')
    defaults_list.each_line do |defaults|
      file = defaults.chomp
      file_path = "#{repodir}/#{file}"
      load_defaults(file_path, belong)
    end

    file_list = %x(git -C #{repodir} ls-files | grep -v 'DEFAULTS').lines
    t_list = []
    10.times do
      t_list << Thread.new do
        while file_list.length > 0
          repo = file_list.shift.chomp
          repo_path = "#{repodir}/#{repo}"
          load_repo_file(repo_path, belong)
        end
      end
    end
    t_list.each do |t|
      t.join
    end
  end

  # 在初始化过程中加载了所有仓库的文件信息
  def load_fork_info
    puts 'start load repo files !'
    @upstreams['upstreams'].each do |repo| # 遍历@upstreams['upstreams']列表中的每个仓库
      traverse_repodir("#{REPO_DIR}/#{repo['location']}", repo['location']) # 加载仓库文件信息
      puts "load #{repo['location']} repo files success !"
    end
    puts 'load ALL repo files success !!!'
  end

  # 创建10个worker线程，每个线程都执行@git_mirror.git_mirror方法
  def create_workers
    @worker_threads = []
    10.times do
      @worker_threads << Thread.new do
        @git_mirror.git_mirror
      end
      sleep(0.1)
    end
  end

  # 发送反馈信息
  def send_message(feedback_info)
    feedback_info.merge!(@git_info[feedback_info[:git_repo]])
    feedback_info.delete(:cur_refs)
    message = feedback_info.to_json # 将反馈信息转换成JSON格式
    @message_queue.publish(message) # 发送到@message_queue队列中
  end

  # 处理反馈信息
  def handle_feedback
    return if @feedback_queue.empty?

    feedback_info = @feedback_queue.pop(true) # 从@feedback_queue队列中获取反馈信息
    git_repo = feedback_info[:git_repo]
    return if check_submodule(git_repo) # 检查是否为子模块

    # values of possible_new_refs:
    # 2: git clone a new repo
    # 1: git fetch and get new refs
    # 0: git fetch and no new refs
    # -1: git fetch fail
    # -2: git clone fail
    update_fork_stat(git_repo, feedback_info[:possible_new_refs]) # 更新仓库状态
    return if feedback_info[:possible_new_refs] < 1

    handle_feedback_new_refs(git_repo, feedback_info) # 处理新提交
  end

  # 将仓库任务放入同步队列@git_queue
  def do_push(fork_key)
    return if @fork_stat[fork_key][:queued]

    # 根据fork_key检查对应的仓库状态，并将仓库信息放入队列中
    @fork_stat[fork_key][:queued] = true
    @git_info[fork_key][:cur_refs] = get_cur_refs(fork_key) if @git_info[fork_key][:cur_refs].nil?
    @git_queue.push(@git_info[fork_key])
  end

  # 将仓库同步任务放入同步队列
  def push_git_queue
    # 检查@git_queue队列是否为空，若不为空，则将队列中的任务取出放入工作线程中处理
    # 若队列为空，则从@priority_queue优先队列中取出优先级最高的仓库任务，并调用do_push方法将其放入同步队列
    return if @git_queue.size >= 1
    return no_repo_warn if @priority_queue.empty?

    fork_key, old_pri = @priority_queue.delete_min
    do_push(fork_key)
    @priority_queue.push fork_key, get_repo_priority(fork_key, old_pri)
    check_worker_threads
  end

  def main_loop
    loop do
      push_git_queue
      handle_feedback
      sleep(0.1)
    end
  end
end

# main thread
class MirrorMain
  # 加载指定目录下的Git仓库文件信息
  def load_repo_file(repodir, belong)
    return unless ascii_text?(repodir) # 检查repodir是否只包含ASCII文本的文件

    git_repo = repodir.delete_prefix("#{REPO_DIR}/#{belong}/") # 提取出git_repo路径
    # 检查 git_repo 是否符合指定的正则表达式（包含小写字母、数字、横杠和下划线，且最多有两级路径）
    return wrong_repo_warn(git_repo) unless git_repo =~ %r{^([a-z0-9]([a-z0-9\-_]*[a-z0-9])*(/\S+){1,2})$}

    git_info = YAML.safe_load(File.open(repodir)) # 加载配置信息
    return if git_info.nil? || git_info['url'].nil? || Array(git_info['url'])[0].nil?

    if File.exist?("#{REPO_DIR}/#{belong}/erb_template") && git_info['erb_enable'] == true
      template = ERB.new File.open("#{REPO_DIR}/#{belong}/erb_template").read
      git_info = YAML.safe_load(template.result(binding))
    end

    # 将加载得到的 git_info 存储在实例变量 @git_info 的哈希中
    git_info['git_repo'] = git_repo
    git_info['belong'] = belong
    git_info = merge_defaults(git_repo, git_info, belong)
    @git_info[git_repo] = git_info

    fork_stat_init(git_repo) # 初始化仓库的状态信息
    @priority_queue.push git_repo, get_repo_priority(git_repo, 0) # 将仓库的优先级信息推入优先队列
  end

  # 比较当前的引用（分支）信息 cur_refs 和旧的引用信息 old_refs，找出有变化的引用
  def compare_refs(cur_refs, old_refs)
    new_refs = { heads: {} }
    cur_refs[:heads].each do |ref, commit_id| # 遍历当前引用的哈希表
      if old_refs[:heads][ref] != commit_id
        new_refs[:heads][ref] = commit_id
      end
    end
    return new_refs
  end

  # 获取指定 git_repo 的当前引用信息
  def get_cur_refs(git_repo)
    return if @git_info[git_repo]['is_submodule'] # 检查git_repo是否为子模块

    mirror_dir = "/srv/git/#{@git_info[git_repo]['belong']}/#{git_repo}.git" # 它根据仓库路径构造出仓库目录 mirror_dir
    show_ref_out = %x(git -C #{mirror_dir} show-ref --heads 2>/dev/null) # 使用git -C命令获取仓库的 show-ref 输出
    cur_refs = { heads: {} }
    show_ref_out.each_line do |line| # 解析出仓库的引用信息保存在哈希 cur_refs 中
      next if line.start_with? '#'

      strings = line.split
      cur_refs[:heads][strings[1]] = strings.first
    end
    return cur_refs
  end

  # 检查指定 git_repo 的新提交
  def check_new_refs(git_repo)
    cur_refs = get_cur_refs(git_repo) # 获取当前引用信息
    # 比较当前引用与之前存储的引用信息 @git_info[git_repo][:cur_refs] 的差异，得到 new_refs
    new_refs = compare_refs(cur_refs, @git_info[git_repo][:cur_refs])
    @git_info[git_repo][:cur_refs] = cur_refs # 更新仓库的当前引用信息
    return new_refs
  end

  # 获取指定 upstream_repos 仓库的变更文件列表
  def get_change_files(upstream_repos)
    old_commit = @git_info[upstream_repos][:cur_refs][:heads]['refs/heads/master'] # 获取旧的commit id
    new_refs = check_new_refs(upstream_repos) # 获取新的提交信息
    new_commit = new_refs[:heads]['refs/heads/master'] # 获取新的提交id
    mirror_dir = "/srv/git/#{@git_info[upstream_repos]['belong']}/#{upstream_repos}.git" # 构造仓库目录
    %x(git -C #{mirror_dir} diff --name-only #{old_commit}...#{new_commit}) # git -C 比较旧的提交和新的提交之间的差异
  end

  # 重新加载指定 upstream_repos 仓库的信息
  def reload_fork_info(upstream_repos)
    if @git_info[upstream_repos][:cur_refs].empty? #  当前仓库的引用信息为空
      @git_info[upstream_repos][:cur_refs] = get_cur_refs(upstream_repos) # 初始化
    else
      changed_files = get_change_files(upstream_repos) # 获取变更文件列表
      reload(changed_files, @git_info[upstream_repos]['belong']) # 重新加载变更的文件信息
    end
  end

  # 重新加载指定文件列表 file_list 中的仓库文件信息
  def reload(file_list, belong)
    system("git -C #{REPO_DIR}/#{belong} pull") # pull更新仓库
    reload_defaults(file_list, belong)
    file_list.each_line do |file| # 依次遍历文件列表，对每个文件进行加载仓库文件信息的操作
      file = file.chomp
      next if File.basename(file) == '.ignore' || File.basename(file) == 'DEFAULTS'

      repo_dir = "#{REPO_DIR}/#{belong}/#{file}"
      load_repo_file(repo_dir, belong) if File.file?(repo_dir)
    end
  end

  # 更新 Elasticsearch 中与指定 git_repo 相关的仓库信息
  def es_repo_update(git_repo)
    # 构造仓库信息哈希
    repo_info = { 'git_repo' => git_repo, 'url' => @git_info[git_repo]['url'] }
    repo_info = repo_info.merge(@fork_stat[git_repo]) # 将 @fork_stat[git_repo] 中的状态信息合并到仓库信息哈希中
    body = repo_info
    # 将仓库信息通过 Elasticsearch 的客户端 @es_client 更新到 Elasticsearch 中
    begin
      @es_client.index(index: 'repo', type: '_doc', id: git_repo, body: body)
    rescue StandardError
      puts $ERROR_INFO
      sleep 1
      retry
    end
  end

  # 更新指定 git_repo 的克隆失败计数和拉取失败计数
  def update_fail_count(git_repo, possible_new_refs)
    @fork_stat[git_repo][:clone_fail_cnt] += 1 if possible_new_refs == -2
    @fork_stat[git_repo][:fetch_fail_cnt] += 1 if possible_new_refs == -1
  end

  # 更新指定 git_repo 的拉取状态信息
  def update_stat_fetch(git_repo)
    @fork_stat[git_repo][:queued] = false # 仓库的队列状态-> false，表示不再排队等待同步任务
    offset_fetch = @fork_stat[git_repo][:offset_fetch] # 拉取偏移量
    offset_fetch = 0 if offset_fetch >= 10
    @fork_stat[git_repo][:fetch_time][offset_fetch] = Time.now.to_s
    @fork_stat[git_repo][:offset_fetch] = offset_fetch + 1
  end

  # 更新指定 git_repo 的新提交状态信息
  def update_new_refs_info(git_repo, offset_new_refs)
    @fork_stat[git_repo][:new_refs_time][offset_new_refs] = Time.now.to_s
    @fork_stat[git_repo][:offset_new_refs] = offset_new_refs + 1
    @fork_stat[git_repo][:new_refs_count] = update_new_refs_count(@fork_stat[git_repo][:new_refs_count]) # 更新新提交的计数信息
  end

  # 更新指定 git_repo 的新提交状态信息
  def update_stat_new_refs(git_repo)
    offset_new_refs = @fork_stat[git_repo][:offset_new_refs] # 当前新提交的偏移量
    offset_new_refs = 0 if offset_new_refs >= 10
    update_new_refs_info(git_repo, offset_new_refs)
  end

  # 在每次镜像同步任务完成后更新仓库的状态信息
  def update_fork_stat(git_repo, possible_new_refs)
    update_stat_fetch(git_repo)
    update_fail_count(git_repo, possible_new_refs)
    git_fail_log(git_repo, possible_new_refs) if possible_new_refs.negative?
    update_stat_new_refs(git_repo) if possible_new_refs.positive? && last_commit_new?(git_repo)
    new_repo_log(git_repo) if possible_new_refs == 2
    es_repo_update(git_repo)
  end
end

# main thread
class MirrorMain
  # 检查指定的 git_repo 是否匹配给定的 webhook_url
  def check_git_repo(git_repo, webhook_url)
    if @git_info.key?(git_repo) # 检查git_info哈希中是否包含git_repo
      git_url = Array(@git_info[git_repo]['url'])[0] # 获取仓库url
      # 如果 git_url 包含了 'compass-ci-robot@'，则去掉该部分并与 webhook_url 进行比较
      return git_url.gsub('compass-ci-robot@', '') == webhook_url if git_url.include?('compass-ci-robot@')

      return git_url == webhook_url
    end
    return false
  end

  # example
  # url: https://github.com/berkeley-abc/abc         git_repo: a/abc/abc
  # url: https://github.com/Siguyi/AvxToNeon         git_repo: a/AvxToNeon/Siguyi
  # 从给定的 webhook_url 中获取对应的 git_repo
  def get_git_repo(webhook_url)
    # 检查webhook_url是否以git_repo开头
    return webhook_url.split(':')[1].gsub(' ', '') if webhook_url =~ /^(git_repo:)/

    # 不是-> 分析 webhook_url 的路径提取出 fork_name 和 project
    fork_name, project = webhook_url.split('/')[-2, 2]

    git_repo = "#{project[0].downcase}/#{project}/#{fork_name}"
    return git_repo if check_git_repo(git_repo, webhook_url) # 检查是否匹配

    if repo_load_fail?(git_repo)
      return git_repo if check_git_repo(git_repo, webhook_url)
    end

    # 再次构造 git_repo 为 project/project
    git_repo = "#{project[0].downcase}/#{project}/#{project}"
    return git_repo if check_git_repo(git_repo, webhook_url) # 检查是否匹配

    if repo_load_fail?(git_repo)
      return git_repo if check_git_repo(git_repo, webhook_url)
    end

    puts "webhook: #{webhook_url} is not found!"
  end

  # 检查指定的 git_repo 是否加载失败
  def repo_load_fail?(git_repo)
    @upstreams['upstreams'].each do |repo| # 遍历所有上游仓库
      file_path = "#{REPO_DIR}/#{repo['location']}/#{git_repo}" # 检查是否存在与 git_repo 对应的文件
      if File.exist?(file_path)
        # 存在-> 加载仓库文件信息
        load_repo_file(file_path, repo['location'])
        return true
      end
    end
    return false
  end

  # 处理webhook消息
  def handle_webhook
    Thread.new do # 独立线程订阅 @webhook_queue 中的消息
      @webhook_queue.subscribe(block: true) do |_delivery, _properties, webhook_url|
        # 有消息到达-> 获取对应git_repo
        git_repo = get_git_repo(webhook_url)
        do_push(git_repo) if git_repo # 将 git_repo 推送到 do_push 方法进行处理
        sleep(0.1)
      end
    end
  end

  # 处理 Pull Request Webhook 消息
  def handle_pr_webhook
    Thread.new do # 独立线程订阅 @webhook_pr_queue 中的消息
      @webhook_pr_queue.subscribe(block: true) do |_delivery, _properties, msg|
        msg = JSON.parse(msg)
        git_repo = get_git_repo(msg['url'])
        next unless git_repo

        mirror_dir = "/srv/git/#{@git_info[git_repo]['belong']}/#{git_repo}.git"
        @git_mirror.git_fetch(mirror_dir)

        update_pr_msg(msg, git_repo)
        @message_queue.publish(msg.to_json)
        sleep 0.1
      end
    end
  end

  # 更新 Pull Request 消息中的提交信息
  def update_pr_msg(msg, git_repo)
    # 将 @git_info[git_repo] 中的信息合并到 msg 中
    @git_info[git_repo].each do |k, v|
      msg[k] = JSON.parse(v.to_json)
    end
    return unless msg['submit_command']

    # 消息中存在提交命令 submit_command0-> 将提交命令和 rpm_name 添加到每个提交的 command 字段中
    msg['submit_command'].each do |k, v|
      msg['submit'].each_index do |t|
        msg['submit'][t]['command'] += " #{k}=#{v}"
        msg['submit'][t]['command'] += " rpm_name=#{git_repo.split('/')[-1]}"
      end
    end
  end

  # 处理子模块信息
  def handle_submodule(submodule, belong)
    submodule.each_line do |line| # 遍历子模块信息的每一行
      next unless line.include?('url = ')

      # 找出url并提取出git_repo
      url = line.split(' = ')[1].chomp
      git_repo = url.split('://')[1] if url.include?('://')
      break unless git_repo

      # 将提取信息存储在git_info哈希中
      @git_info[git_repo] = { 'url' => url, 'git_repo' => git_repo, 'is_submodule' => true, 'belong' => belong }
      fork_stat_init(git_repo) # 初始化子模块的状态信息
      @priority_queue.push git_repo, get_repo_priority(git_repo, 0)
    end
  end

  # 检查指定的 git_repo 是否是子模块
  def check_submodule(git_repo)
    if @git_info[git_repo]['is_submodule'] # 是子模块
      @fork_stat[git_repo][:queued] = false
      return true
    end

    mirror_dir = "/srv/git/#{@git_info[git_repo]['belong']}/#{git_repo}.git"
    submodule = %x(git -C #{mirror_dir} show HEAD:.gitmodules 2>/dev/null) # 获取仓库的子模块信息
    return if submodule.empty?

    handle_submodule(submodule, @git_info[git_repo]['belong']) # 存在子模块信息
  end

  def get_fork_stat(git_repo)
    fork_stat = {
      queued: false,
      fetch_fail_cnt: 0,
      clone_fail_cnt: 0,
      fetch_time: [],
      offset_fetch: 0,
      new_refs_time: [],
      offset_new_refs: 0,
      new_refs_count: {},
      last_commit_time: 0
    }
    query = { query: { match: { _id: git_repo } } }
    # 检查 Elasticsearch 中是否存在与 git_repo 对应的文档
    return fork_stat unless @es_client.count(index: 'repo', body: query)['count'].positive?

    begin
      # 存在-> 获取文档信息
      result = @es_client.search(index: 'repo', body: query)['hits']
    rescue StandardError
      puts $ERROR_INFO
      sleep 1
      retry
    end

    fork_stat.each_key do |key|
      # 与默认的 fork_stat 哈希进行合并
      fork_stat[key] = result['hits'][0]['_source'][key.to_s] || fork_stat[key]
    end
    return fork_stat
  end

  def create_year_hash(new_refs_count, year, month, day)
    new_refs_count[year] = 1
    new_refs_count[month] = 1
    new_refs_count[day] = 1
    return new_refs_count
  end

  def update_year_hash(new_refs_count, year, month, day)
    new_refs_count[year] += 1
    return create_month_hash(new_refs_count, month, day) if new_refs_count[month].nil?

    return update_month_hash(new_refs_count, month, day)
  end

  def create_month_hash(new_refs_count, month, day)
    new_refs_count[month] = 1
    new_refs_count[day] = 1

    return new_refs_count
  end

  def update_month_hash(new_refs_count, month, day)
    new_refs_count[month] += 1
    if new_refs_count[day].nil?
      new_refs_count[day] = 1
    else
      new_refs_count[day] += 1
    end
    return new_refs_count
  end

  def update_new_refs_count(new_refs_count)
    t = Time.now

    # example: 2021-01-28
    day = t.strftime('%Y-%m-%d')
    # example: 2021-01
    month = t.strftime('%Y-%m')
    # example: 2021
    year = t.strftime('%Y')
    return create_year_hash(new_refs_count, year, month, day) if new_refs_count[year].nil?

    return update_year_hash(new_refs_count, year, month, day)
  end
end

# main thread
class MirrorMain
  STEP_SECONDS = 2592000 # 每个步骤的时间间隔（用于计算仓库的优先级）

  def handle_feedback_new_refs(git_repo, feedback_info)
    # 检查git_repo是否是上游仓库
    return reload_fork_info(git_repo) if upstream_repo?(git_repo) #是-> 重新加载该仓库的信息

    # 检查git_repo是否是社区仓库
    return handle_community_sig(git_repo) if community?(git_repo) #是-> 处理社区的提交信息

    new_refs = check_new_refs(git_repo) #否-> 检查该仓库的新提交信息
    return if new_refs[:heads].empty?

    feedback_info[:new_refs] = new_refs #有新提交-> 将新提交信息添加到 feedback_info 中
    send_message(feedback_info) # 发送消息
    # 最后一次提交是新的-> 记录新提交的日志
    new_refs_log(git_repo, new_refs[:heads].length) if last_commit_new?(git_repo)
  end

  # 根据 object_key 和默认设置 @defaults 中的信息进行合并，并返回合并后的对象
  def merge_defaults(object_key, object, belong)
    return object if object_key == belong

    defaults_key = File.dirname(object_key)
    while defaults_key != '.'
      return @defaults[defaults_key].merge(object) if @defaults[defaults_key]

      defaults_key = File.dirname(defaults_key)
    end
    return @defaults[belong].merge(object) if @defaults[belong]

    return object
  end

  def clone_upstream_repo
    if File.exist?('/etc/compass-ci/defaults/upstream-config')  # 从默认配置文件中加载上游仓库的信息
      @upstreams = YAML.safe_load(File.open('/etc/compass-ci/defaults/upstream-config'))
      @upstreams['upstreams'].each do |repo| # 遍历每个上游仓库的信息
        url = get_url(repo['url'])
        %x(git clone #{url} #{REPO_DIR}/#{repo['location']} 2>&1) # 将仓库克隆到指定位置
      end
    else
      puts 'ERROR: No upstream-config file'
      return -1
    end
  end

  def get_url(url)
    if url.include?('gitee.com/') && File.exist?("/srv/git/#{url.delete_prefix('https://')}")
      url = "/srv/git/#{url.delete_prefix('https://')}"
    end
    return url
  end

  def upstream_repo?(git_repo)
    @upstreams['upstreams'].each do |repo|
      return true if git_repo == repo['git_repo']
    end
    return false
  end

  def community?(git_repo)
    return true if git_repo == "c/community/community"
    return false
  end

  # 处理社区提交的信息
  def handle_community_sig(git_repo)
    change_files = get_change_files(git_repo) # 获取变更的文件列表
    change_files.each_line do |line|
      next unless line =~ %r{^sig/(\S+)/src-openeuler/(\S+)yaml$}

      add_openeuler_repo(line.chomp, git_repo)
    end
  end

  def add_openeuler_repo(yaml_file, git_repo)
    # 从YAML文件中提取仓库名name
    name = %x(git -C /srv/git/openeuler/#{git_repo}.git show HEAD:#{yaml_file}).lines[0].chomp.gsub('name: ','')
    first_letter = name.downcase.chars.first
    repo_path = "#{REPO_DIR}/openeuler/#{first_letter}/#{name}" # 根据名称的首字母创建对应的目录

    # 在目录中创建包含仓库 URL 信息的 YAML 文件
    FileUtils.mkdir_p(repo_path, mode: 0o775)
    File.open("#{repo_path}/#{name}", 'w') do |f|
      f.write({ 'url' => Array("https://gitee.com/src-openeuler/#{name}") }.to_yaml)
    end

    %x(git -C #{REPO_DIR}/openeuler add #{first_letter}/#{name}/#{name})
    %x(git -C #{REPO_DIR}/openeuler commit -m "add repo src-openeuler/#{name}")
    %x(git -C #{REPO_DIR}/openeuler push)

    load_repo_file("#{repo_path}/#{name}", "openeuler") # 加载仓库文件信息
    do_push("#{first_letter}/#{name}/#{name}")
  end

  # 重新加载默认设置
  def reload_defaults(file_list, belong)
    file_list.each_line do |file|
      file = file.chomp
      next unless File.basename(file) == 'DEFAULTS' # 查找并加载 DEFAULTS 文件

      repodir = "#{REPO_DIR}/#{belong}/#{file}"
      load_defaults(repodir, belong)
      traverse_repodir(repodir, belong) # 递归调用 traverse_repodir 方法遍历加载仓库信息
    end
  end

  def new_repo_log(git_repo)
    @log.info({
                msg: 'new repo',
                repo: git_repo
              })
  end

  def git_fail_log(git_repo, possible_new_refs)
    msg = possible_new_refs == -1 ? 'git fetch fail' : 'git clone fail'
    @log.info({
                msg: msg,
                repo: git_repo
              })
  end

  def new_refs_log(git_repo, nr_new_branch)
    @log.info({
                msg: 'new refs',
                repo: git_repo,
                nr_new_branch: nr_new_branch
              })
  end

  def worker_threads_warn(alive)
    @log.warn({
                state: 'some workers died',
                alive_num: alive
              })
  end

  def worker_threads_error(alive)
    @log.error({
                 state: 'most workers died',
                 alive_num: alive
               })
  end

  def wrong_repo_warn(git_repo)
    @log.warn({
                msg: 'wrong repos',
                repo: git_repo
              })
  end

  def no_repo_warn
    @log.warn({
                msg: 'no repo files'
              })
  end

  def last_commit_new?(git_repo)
    inactive_time = %x(git -C /srv/git/#{@git_info[git_repo]['belong']}/#{git_repo}.git log --pretty=format:"%cr" -1)
    return false if inactive_time =~ /(day|week|month|year)/

    return true
  end

  # 获取仓库的优先级
  def get_repo_priority(git_repo, old_pri)
    old_pri ||= 0
    mirror_dir = "/srv/git/#{@git_info[git_repo]['belong']}/#{git_repo}" # 仓库在本地服务器的镜像目录位置
    mirror_dir = "#{mirror_dir}.git" unless @git_info[git_repo]['is_submodule'] # 是子模块-> 以.git结尾

    step = (@fork_stat[git_repo][:clone_fail_cnt] + 1) * Math.cbrt(STEP_SECONDS)
     # mirror_dir对应目录不存在（即文件夹不存在）-> 还没clone
    return old_pri + step unless File.directory?(mirror_dir) # 返回默认step

    return cal_priority(mirror_dir, old_pri, git_repo)
  end

  # 根据仓库最后一次提交的时间与当前时间的间隔来计算优先级
  def cal_priority(_mirror_dir, old_pri, git_repo)
    last_commit_time = @fork_stat[git_repo][:last_commit_time]
    step = (@fork_stat[git_repo][:fetch_fail_cnt] + 1) * Math.cbrt(STEP_SECONDS)
    return old_pri + step if last_commit_time.zero? # last_commit_time为零-> 没进行过提交，返回默认step

    # 最近更新的仓库优先级较高，从而更快地获取到更新
    t = Time.now.to_i
    interval = t - last_commit_time # 计算当前时间与最后一次提交时间之间的时间间隔
    return old_pri + step if interval <= 0

    return old_pri + Math.cbrt(interval)
  end

  # 检查worker线程状态
  def check_worker_threads
    alive = 0
    @worker_threads.each do |t|
      alive += 1 if t.alive? # 遍历统计当前存活的线程数
    end
    num = @worker_threads.size
    return worker_threads_error(alive) if alive < num / 2 # 存活的线程数量小于总线程数的一半-> 记录一个错误日志
    return worker_threads_warn(alive) if alive < num # 存活的线程数量小于总线程数，但大于一半-> 记录一个警告日志
  end
end

# main thread
class MirrorMain
  # 检查给定的文件是否包含ASCII文本
  def ascii_text?(file_name)
    type = %x(file "#{file_name}").chomp.gsub("#{file_name}: ", '') # 将输出中的文件名前缀删除
    return true if type == 'ASCII text'

    return false
  end
end
