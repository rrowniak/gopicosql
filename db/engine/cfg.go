package engine

func NewConfigDefault() *Cfg {
	return &Cfg{
		DbDir:            ".",
		CompactEverySecs: 60,
		ServHost:         "127.0.0.1",
		ServPort:         8080,
		MaxRestRequests:  10,
		MaxDbRequests:    10,
		QueryTimeoutSecs: 30,
	}
}

func NewConfig(filename string) *Cfg {
	panic("Not implemented")
}

type Cfg struct {
	DbDir            string
	CompactEverySecs int
	ServHost         string
	ServPort         int
	MaxRestRequests  int
	MaxDbRequests    int
	QueryTimeoutSecs int
}
