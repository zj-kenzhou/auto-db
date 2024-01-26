package datasource

import (
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
	"time"
)

func createConfigWithLog(logLevel int) *gorm.Config {
	return &gorm.Config{
		Logger: logger.Default.LogMode(logger.LogLevel(logLevel)),
	}
}

func setDbProperties(db *gorm.DB, dbConfig Config) {
	sqlDB, err := db.DB()
	if err != nil {
		panic(err)
	}
	sqlDB.SetMaxIdleConns(dbConfig.MaxIdleConns)
	sqlDB.SetMaxOpenConns(dbConfig.MaxOpenConns)
	sqlDB.SetConnMaxLifetime(time.Duration(dbConfig.ConnMaxLifetime) * time.Minute)
	sqlDB.SetConnMaxIdleTime(time.Duration(dbConfig.ConnMaxIdleTime) * time.Minute)
}

func AutoMigrate(dst ...interface{}) {
	AutoMigrateWithName(_primary, dst...)
}

func AutoMigrateWithName(name string, dst ...interface{}) {
	config, found := _configMap[name]
	if !found {
		log.Println("datasource config not found")
		return
	}
	if config.AutoMigrate {
		db := GetDb(name)
		if db == nil {
			log.Println(ErrDatasourceNotFound.Error())
		}
		if err := db.AutoMigrate(dst...); err != nil {
			log.Println(err)
		}
	}
}
