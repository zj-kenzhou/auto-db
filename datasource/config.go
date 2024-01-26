package datasource

type Config struct {

	// 数据库类型
	Type string

	// 数据库url
	Host string

	// 数据库Username
	Username string

	// 数据库Password
	Password string

	// Silent 1 Error 2 Warn 3 Info 4
	LogLevel int

	// 数据库名称
	Dbname string

	// 连接池中最大空闲连接的数量  n <= 0 代表没有空闲
	MaxIdleConns int

	// 打开数据库连接的最大数量 n <= 0 代表不限制
	MaxOpenConns int

	// 连接可复用的最大时间(单位分钟) n <= 0 则不关闭连接
	ConnMaxLifetime int

	// 连接空闲的最大时间(单位分钟) n <= 0 则不关闭连接
	ConnMaxIdleTime int

	// 自动创建数据表
	AutoMigrate bool
}
