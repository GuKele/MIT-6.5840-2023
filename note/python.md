在ubuntu和debian中安装全局python module通常使用apt来安装，使用pip会提示系统建议使用系统管理器apt。
``` bash
apt install python3-<module-name>
```

如果想要使用pip安装，通常做法是创建一个虚拟的python环境，然后激活并在其中使用pip安装所需的包
```` bash
# 创建一个名为 'venv' 的虚拟环境
python3 -m venv venv

# 激活虚拟环境
source venv/bin/activate
```
