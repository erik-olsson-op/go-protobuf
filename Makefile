# Compile all .proto files in the proto dir from root
proto-compile:
		protoc --go_out=. ./proto/*.proto