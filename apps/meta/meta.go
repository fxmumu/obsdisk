package meta

import (
	"errors"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

type Meta struct {
	dbFile string
	db     *gorm.DB
}

type Vol struct {
	gorm.Model `json:"-"`
	Name       string `json:"name"`
	ObsType    string `json:"obsType"`
}

type Option func(meta *Meta)

func DbFile(file string) Option {
	return func(meta *Meta) {
		meta.dbFile = file
	}
}

func New(opts ...Option) (*Meta, error) {
	m := &Meta{}
	for _, opt := range opts {
		opt(m)
	}
	if m.dbFile == "" {
		return nil, errors.New("db file should set")
	}
	db, err := gorm.Open(sqlite.Open(m.dbFile), &gorm.Config{})
	if err != nil {
		return nil, err
	}
	err = db.Migrator().AutoMigrate(&Vol{})
	if err != nil {
		return nil, err
	}
	m.db = db
	return m, nil
}

func (m *Meta) NewDisk(diskName, obsType string) error {
	return m.db.Create(&Vol{Name: diskName, ObsType: obsType}).Error
}

func (m *Meta) DiskExisted(diskName string) (bool, error) {
	var nums int64
	err := m.db.Table("vols").Where("name = ?", diskName).Count(&nums).Error
	if err != nil {
		return false, err
	}
	return nums > 0, nil
}

func (m *Meta) Disks() ([]Vol, error) {
	vols := make([]Vol, 0)
	err := m.db.Table("vols").Scan(&vols).Error
	if err != nil {
		return nil, err
	}
	return vols, nil
}
