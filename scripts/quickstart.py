from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
def WriteGoogleSheets(SpreadsheetLocation):
	gauth = GoogleAuth()
	gauth.LocalWebserverAuth() # Creates local webserver and auto handles authentication.

	drive = GoogleDrive(gauth)
	Spreadsheet=SpreadsheetLocation[:SpreadsheetLocation.rfind('.')]
	print(Spreadsheet)
	SpreadsheetName=SpreadsheetLocation.rsplit('/', 1)[-1]	
	print(SpreadsheetName)

	test_file = drive.CreateFile({SpreadsheetLocation: SpreadsheetName})
	test_file.SetContentFile(SpreadsheetLocation)
	test_file.Upload({'convert': True})

	# Insert the permission.
	permission = test_file.InsertPermission({
	                        'type': 'anyone',
	                        'value': 'anyone',
	                        'role': 'reader'})

	print(test_file['alternateLink'])  # Display the sharable link.


WriteGoogleSheets('/Users/scgordon/MetadataEvaluation/data/AND/AND_Report.xlsx')	