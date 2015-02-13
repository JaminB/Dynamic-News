f = open('pronouns','r')
for line in f:
	print("INSERT INTO pronouns VALUES('" + line.replace('\n','') + "');")
