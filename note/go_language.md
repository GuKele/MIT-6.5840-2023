## 变量

### 指针
go的指针也和c++不一样，好像是int const *，是个顶层const

## 结构体

### struct

go语言中成员（函数、变量）首字母大写表示public，首字母小写表示private

```go
type StructName struct {
   member Definition1
   member Definition2
   ...
   member definition3
}
```
### ENUM
```go
// stringer -type=FishType 该命令会在同文件夹下创建fish_type_string.go文件，包含该类型的String()函数。
// 或者使用go::generate指令标记，运行go generate
// go:generate stringer -type=FishType
type FishType int // 使用自定义类型来实现更安全的枚举

const (
  A FishType = iota
  B
  C
  D
)
```

## 运算符
``` go
&^ # (位清移)运算符 对比两个二进制数，相同位一致当前位取 0，相同位不一致取 左边变量二进制数的当前位值.所以可以用来清零！
```

## function
``` go
func function_name( [parameter list] ) [return_types] {
  函数体
}

// 成员函数则：
func (ob *ClassType) function_name( [parameter list] ) [return_types] {
  函数体
}
```
go语言传参都是按值复制(传指针也是把指针复制一遍)，只不过go语言里头的map、channel等复制不像c++的拷贝构造函数一样深拷贝，而是一个浅拷贝，所以让人感觉像是传递了引用一样。这点类似于java，但是注意如果一个map、channel没有初始化，即nil，那么你传递到函数中进行修改不会影响到外面，因为浅拷贝的是nil。

但是go语言的slice就有点特殊了，go语言的slice和c++的vector的实现不一样，c++ vector是三个指针(begin, end, cap); 而go的slice是一个指针+两个int(begin, int len, int cap)，这就导致如果你传值，那么你在里头修改已经存在的元素则没问题，但是如果你增删元素， 那么len和cap是不会改变的，甚至当你发生扩容的时候，那个新begin指针也不会改变。所以需要传递slice的指针。

## switch
switch的case语句自带break;

## 数组(栈上)
```go
var array_name [size]data_type
array_name := [size]data_type{initial list}
array_name := [...]data_type{initial list} // 编译器推断数组大小
```

## 切片(动态数组)
go语言中，切片操作并不是复制，而是对原来数组的引用，所以导致底层的完整数组不会被释放。（虽然不同于内存泄漏，但是没有引用到的数据也不会被释放）
go语言的slice就有点特殊了，go语言的slice和c++的vector的实现不一样，c++ vector是三个指针(begin, end, cap); 而go的slice是一个指针+两个int(begin, int len, int cap)，这就导致如果你传值，那么你在里头修改已经存在的元素则没问题，但是如果你增删元素， 那么len和cap是不会改变的，甚至当你发生扩容的时候，那个新begin指针也不会改变。所以需要传递slice的指针。
``` go
var slice1 []type = make([]type, len)
var slice1 []type = make([]T, length, capacity) // make 也可以指定capacity，
slice1 := make([]type, len)
len() 和 cap() 函数
```
## map
```go
map_variable := make(map[KeyType]ValueType, initialCapacity)
m := map[string]int{
  "apple": 1,
  "banana": 2,
  "orange": 3,
}

v1 := m["apple"]
v2, ok := m["pear"]  // 如果键不存在，ok 的值为 false，v2 的值为该类型的零值

// 获取 Map 的长度
len := len(m)

// 删除键值对
delete(m, "banana")

// 添加元素
// 调用append函数必须用原来的切片变量接收返回值
slice1 = append(slice1, elem1, elem2等)
slice1 = append(slice1, slice2...) // 有点像c++包展开
```

## range
Go 语言中 range 关键字用于 for 循环中迭代数组(array)、切片(slice)、通道(channel)或集合(map)的元素。在数组和切片中它返回元素的索引和索引对应的值，在集合中返回 key-value 对。
```go
for key, value := range oldMap {
  newMap[key] = value
}

array := []int{1, 2, 4, 8, 16, 32, 64, 128}
for idx, val := range array {
  fmt.Printf("2**%d = %d\n", idx, val)
}

slice := make([]int, 10, 20)
for idx, val := range slice {

}
```

## lambda
``` go
// 似乎全是隐式的引用捕获，不像c++还得需要自己捕获并且选择捕获类型，如果需要按值那就传参
func (参数列别)(返回值列表) {

}

sum := func(int a, int b) (int) {
  return a + b
}
```
## channel
对于无缓冲，或者有缓冲channel缓冲区不够用时，就会阻塞。
容易产生bug的是，当你加锁，然后ch <- ，接收方也是先加锁，然后 <-ch，这时候写channel的一方在因为channel而堵塞，没有释放锁，而接收方又需要先拿锁才能读channel，一个死锁就这样产生了...
``` go
ch1 := make(chan int)    // 无缓冲区
ch2 := make(chan int, 4)    // 缓冲区大小为4，可以写入4个int
ch3 := ch2     // channel是引用类型
```

## condition variant
condition variant中的锁应该是保护临界区资源的，条件变量本身应该是不需要锁的，应该是和c++一样，但是还是wait会释放锁，所以条件变量中的锁不能为nil，也必须在wait前进行lock。
``` go
sync.Cond
sync.NewCond()
```

## 包
go get golang.org/xx来下载
go install 安装

### bytes
### encoding/gob
### strconv
strconv is a package in the Go programming language's standard library that provides functions to convert between strings and numeric values. It includes functions for parsing strings as integers, floats, and other numeric types.

## 输出格式
``` go
%v 值的默认格式
%+v 类似上面，但是输出结构体时会添加字段名字
%#v 值的go语法表示
%T 值的类型的go语法表示
%% 百分号
%t 单词true或者false
```

## 特殊

### init 函数
init() 函数是 Go 语言中的一个特殊函数。它没有参数，也没有返回值，它的作用是在程序运行开始时自动执行一些初始化操作。在一个包中，可以定义多个 init() 函数，它们会按照它们在源代码中的顺序被调用。

在 Go 语言中，init() 函数的使用场景主要有两个方面：

初始化包：init() 函数通常用来初始化包，执行一些全局的初始化操作。比如，你可以在 init() 函数中打开数据库连接、初始化全局变量、加载配置文件等。
注册：在某些情况下，你可能希望在程序启动时执行一些特定的操作，但不希望依赖于包的导入顺序。这时可以使用 init() 函数来注册需要在程序启动时执行的代码。
