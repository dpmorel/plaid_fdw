MODULE_NAME = dnb_psql

install:
	@pip install .

uninstall:
	@pip uninstall $(MODULE_NAME)

clean:
	@rm -f *.pyc
