## 结构体

### struct
```go
type StructName struct {
   member definition
   member definition
   ...
   member definition
}
```
### ENUM
```go
type FishType int // 或者其他类型
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

## switch
switch的case语句自带break;

## 数组(栈上)
```go
var array_name [size]data_type
array_name := [size]data_type{initial list}
array_name := [...]data_type{initial list} // 编译器推断数组大小
```

## 切片(动态数组)
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
```

## range
```go
// Go 语言中 range 关键字用于 for 循环中迭代数组(array)、切片(slice)、通道(channel)或集合(map)的元素。在数组和切片中它返回元素的索引和索引对应的值，在集合中返回 key-value 对。
for key, value := range oldMap {
  newMap[key] = value
}

pow := []int{1, 2, 4, 8, 16, 32, 64, 128}
for idx, val := range pow {
  fmt.Printf("2**%d = %d\n", idx, val)
}

```

## lambda
``` go
// 似乎全是隐士的引用捕获，不像c++还得需要自己捕获并且选择捕获类型，如果需要按值那就传参
func (参数列别)(返回值列表) {

}

sum := func(int a, int b) (int) {
  return a + b
}
```

## 包
### bytes
### encoding/gob
### strconv
strconv is a package in the Go programming language's standard library that provides functions to convert between strings and numeric values. It includes functions for parsing strings as integers, floats, and other numeric types.
