package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
	"github.com/glebarez/sqlite"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type Vol struct {
	gorm.Model `json:"-"`
	Name       string `json:"name"`
	ObsType    string `json:"obsType"`
}

var w, h float32 = 600, 400
var header = []string{"DiskName", "CreatTime", "ObsType", "Actions"}
var volsMap sync.Map

func main() {
	currentUser, err := user.Current()
	if err != nil {
		logrus.Fatalf("get current user failed: %s", err.Error())
	}
	if strings.ToLower(currentUser.Name) == "root" {
		logrus.Fatal("don't run as root")
	}
	workDir := path.Join(currentUser.HomeDir, "ObsDisk")

	aPath, err := os.Executable()
	if err != nil {
		logrus.Error(err)
		return
	}
	jfs := filepath.Join(filepath.Dir(aPath), "juicefs")
	// var _ = aPath
	// jfs := filepath.Join("/Users/hugonglin/Downloads/juicefs-1.0.0-darwin-amd64", "juicefs")

	var dirNeeds []string
	dirNeeds = append(dirNeeds, workDir)
	iniDir := path.Join(workDir, "ini")
	dirNeeds = append(dirNeeds, iniDir)
	volsDir := path.Join(workDir, "vols")
	dirNeeds = append(dirNeeds, volsDir)
	metasDir := path.Join(workDir, "metas")
	dirNeeds = append(dirNeeds, metasDir)
	for _, dirNeed := range dirNeeds {
		if err := ensureDir(dirNeed); err != nil {
			logrus.Error(err)
			return
		}
	}

	db, err := gorm.Open(sqlite.Open(path.Join(iniDir, "disks")), &gorm.Config{})
	if err != nil {
		return
	}
	err = db.Migrator().AutoMigrate(&Vol{})
	if err != nil {
		return
	}

	a := app.New()
	window := a.NewWindow("ObsDisk")
	newDiskWindow := a.NewWindow("new obs disk")
	// don't close the window really, or it can't open again
	newDiskWindow.SetCloseIntercept(func() { newDiskWindow.Hide() })
	window.SetMaster()
	window.Resize(fyne.Size{Width: w, Height: h})

	diskNameEntry := widget.NewEntry()
	diskNameItem := widget.NewFormItem("DiskName", diskNameEntry)
	akItemEntry := widget.NewEntry()
	akItem := widget.NewFormItem("AccessKey", akItemEntry)
	skItemEntry := widget.NewPasswordEntry()
	skItem := widget.NewFormItem("AccessKey Secret", skItemEntry)
	bucketItemEntry := widget.NewEntry()
	bucketItem := widget.NewFormItem("Bucket", bucketItemEntry)

	formSubmit := func() {
		diskName := strings.TrimSpace(diskNameEntry.Text)
		ak := strings.TrimSpace(akItemEntry.Text)
		sk := strings.TrimSpace(skItemEntry.Text)
		bucket := strings.TrimSpace(bucketItemEntry.Text)
		if diskName == "" {
			dialog.ShowError(fmt.Errorf("empty DiskName"), newDiskWindow)
			return
		}
		if ak == "" {
			dialog.ShowError(fmt.Errorf("empty AccessKey"), newDiskWindow)
			return
		}
		if sk == "" {
			dialog.ShowError(fmt.Errorf("empty AccessKey Secret"), newDiskWindow)
			return
		}
		if bucket == "" {
			dialog.ShowError(fmt.Errorf("empty Bucket"), newDiskWindow)
			return
		}
		obsType, err := parseObsTypeFromBucket(bucket)
		if err != nil {
			dialog.ShowError(fmt.Errorf("parse obs type from bucket failed: %s", err.Error()), newDiskWindow)
			return
		}
		existed, err := diskExisted(db, diskName)
		if err != nil {
			dialog.ShowError(fmt.Errorf("check DiskName exist failed: %s", err.Error()), newDiskWindow)
			return
		}
		if existed {
			dialog.ShowError(fmt.Errorf("DiskName %s existed", diskName), newDiskWindow)
			return
		}
		meta := path.Join(metasDir, diskName)
		cmd := exec.Command(jfs,
			"format",
			"--trash-days", "0",
			"--access-key", ak,
			"--secret-key", sk,
			"--bucket", bucket,
			"--storage", obsType,
			fmt.Sprintf("sqlite3://%s", meta),
			diskName)
		var stdout, stderr bytes.Buffer
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
		if err := cmd.Run(); err != nil {
			os.Remove(meta)
			dialog.ShowError(fmt.Errorf("format err: %s", extraFatalErr(string(stderr.Bytes()))), newDiskWindow)
			return
		}
		err = db.Create(&Vol{Name: diskName, ObsType: obsType}).Error
		if err != nil {
			os.Remove(meta)
			dialog.ShowError(fmt.Errorf("record disk err: %s", err), newDiskWindow)
			return
		}
		dialog.ShowInformation("success", "submit success", newDiskWindow)
	}

	form := &widget.Form{
		Items:    []*widget.FormItem{diskNameItem, akItem, skItem, bucketItem},
		OnSubmit: formSubmit,
	}

	var data [][]string
	data = append(data, header)
	vols, err := disks(db)
	if err != nil {
		logrus.Error(err)
		return
	}
	for _, vol := range vols {
		_, exist := volsMap.Load(vol.Name)
		if exist {
			continue
		}
		volsMap.Store(vol.Name, struct{}{})
		l := []string{vol.Name, vol.CreatedAt.Format(time.RFC3339), vol.ObsType, ""}
		data = append(data, l)
	}

	table := widget.NewTable(
		func() (int, int) {
			return len(data), len(data[0])
		},
		func() fyne.CanvasObject {
			lb := widget.NewLabel("")
			toolbar := widget.NewToolbar()
			toolbar.Hide()
			return container.NewHBox(lb, toolbar)
		},
		func(i widget.TableCellID, o fyne.CanvasObject) {
			c := o.(*fyne.Container)
			lb := c.Objects[0].(*widget.Label)
			toolbar := c.Objects[1].(*widget.Toolbar)
			if i.Col == len(header)-1 {
				lb.Hide()
				toolbar.Hidden = false
				if len(toolbar.Items) == 0 {
					volName := data[i.Row][0]
					mountPoint := path.Join(volsDir, volName)
					meta := path.Join(metasDir, volName)
					toolbar.Append(widget.NewToolbarAction(theme.DownloadIcon(), func() {
						cmd := exec.Command(jfs, "mount", "-d", fmt.Sprintf("sqlite3://%s", meta), mountPoint)
						var stdout, stderr bytes.Buffer
						cmd.Stdout = &stdout
						cmd.Stderr = &stderr
						err := cmd.Run()
						if err != nil {
							dialog.ShowError(fmt.Errorf("err %s", extraFatalErr(string(stderr.Bytes()))), window)
							return
						}
						dialog.ShowInformation("tips", "mount success", window)
					}))
					toolbar.Append(widget.NewToolbarAction(theme.UploadIcon(), func() {
						cmd := exec.Command(jfs, "umount", mountPoint)
						var stdout, stderr bytes.Buffer
						cmd.Stdout = &stdout
						cmd.Stderr = &stderr
						err := cmd.Run()
						if err != nil {
							dialog.ShowError(fmt.Errorf("err %s", extraFatalErr(string(stderr.Bytes()))), window)
							return
						}
						dialog.ShowInformation("tips", "umount success", window)
					}))
				}
			} else {
				toolbar.Hide()
				lb.Hidden = false
				lb.SetText(data[i.Row][i.Col])
			}
		})

	colWidths := []float32{100, 200, 100}
	for i, w := range colWidths {
		table.SetColumnWidth(i, w)
	}

	tk := time.NewTicker(time.Second * 1)
	go func() {
		for true {
			select {
			case <-tk.C:
				vols, err := disks(db)
				if err != nil {
					logrus.Error(err)
					return
				}
				for _, vol := range vols {
					_, exist := volsMap.Load(vol.Name)
					if exist {
						continue
					}
					volsMap.Store(vol.Name, struct{}{})
					l := []string{vol.Name, vol.CreatedAt.Format(time.RFC3339), vol.ObsType, ""}
					data = append(data, l)
				}
				table.Refresh()
			}
		}
	}()

	t := widget.NewToolbar(widget.NewToolbarAction(theme.ContentAddIcon(), func() {
		addSize := fyne.NewSize(w*0.8, h/2)
		newDiskWindow.Resize(addSize)
		newDiskWindow.SetContent(form)
		newDiskWindow.Show()
	}))
	bars := container.NewHBox(t)
	split := container.NewVSplit(bars, table)
	split.Offset = 0.1
	window.SetContent(split)
	window.Show()

	a.Run()
}

// obs url keyword:obs schema
var obsSchemaMap = map[string]string{
	"aliyuncs":      "oss",
	"myhuaweicloud": "obs",
	"myqcloud":      "cos",
}

func diskExisted(db *gorm.DB, diskName string) (bool, error) {
	var nums int64
	err := db.Table("vols").Where("name = ?", diskName).Count(&nums).Error
	if err != nil {
		return false, err
	}
	return nums > 0, nil
}

func disks(db *gorm.DB) ([]Vol, error) {
	vols := make([]Vol, 0)
	err := db.Table("vols").Scan(&vols).Error
	if err != nil {
		return nil, err
	}
	return vols, nil
}

func parseObsTypeFromBucket(bucket string) (string, error) {
	var schema string
	for k, s := range obsSchemaMap {
		if strings.Contains(bucket, k) {
			schema = s
			break
		}
	}
	if schema == "" {
		return "", fmt.Errorf("unsupport bucket %s", bucket)
	}
	return schema, nil
}

func extraFatalErr(ori string) string {
	if ori == "" {
		return ""
	}
	re := regexp.MustCompile(`(?s)<FATAL>:(.*)`)
	match := re.FindStringSubmatch(ori)
	if len(match) >= 2 {
		return match[1]
	}
	return ori
}

func ensureDir(dirName string) error {
	err := os.MkdirAll(dirName, 0755)
	if err == nil {
		return nil
	}
	if os.IsExist(err) {
		info, err := os.Stat(dirName)
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return errors.New("path exists but is not a directory")
		}
		return nil
	}
	return err
}
