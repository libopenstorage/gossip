ifndef PKGS
PKGS := $(shell go list ./... 2>&1)
endif

all: test

test:
	ifconfig lo:2 127.0.0.2 netmask 255.255.255.0 up
	ifconfig lo:3 127.0.0.3 netmask 255.255.255.0 up
	ifconfig lo:4 127.0.0.4 netmask 255.255.255.0 up
	ifconfig lo:5 127.0.0.5 netmask 255.255.255.0 up
	ifconfig lo:6 127.0.0.6 netmask 255.255.255.0 up

	cd proto && go test

	ifconfig lo:2 down
	ifconfig lo:3 down
	ifconfig lo:4 down
	ifconfig lo:5 down
	ifconfig lo:6 down
