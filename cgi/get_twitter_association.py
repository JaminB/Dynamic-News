__author__ = 'jamin'
#!/usr/bin/python3
import sys
import cgi
def GET():
	sys.path.append('../')
	import interfaces
	form = cgi.FieldStorage()
	print("Content-type:text/json\r\n\r\n")
	if 'mindate' not in form:
		print('{ "error": "Please fill in mindate field." }')
		return
	else:
		mindate = str(form['mindate'].value)
		interfaces.Correlate(mindate).print_json_response()

GET()