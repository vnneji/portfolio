########################## Amplitude Data Extraction #######################
##### This program is created to download Amplitude Metrics Data  for a user's deployed and Amplitude-integrated app
##### The data is downloaded in its raw format for local analysis or storage in a data warehouse
##### To use this program, the user must have access to the production environment of the Amplitude integration for the app
##### From there, he can download the authorisation token and get the project's key Code

###Functions
##Download Function
#This function downloads the amplitude metrics data for the app for one day of observations
Function Get-Amplitude{
    Param(
        $authKey,
        $nextDate,
        $downloadFile
        )

$authKey = "Basic "+ $authKey
$header = @{"Authorization"= $authKey
"User-Agent" = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36 Edg/109.0.1518.78"
"Host" = "analytics.amplitude.com"
"authority"="analytics.amplitude.com"
"method"="GET"
"scheme"="https"
"accept"="text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9"
"accept-encoding"="gzip, deflate, br"
"accept-language"="en-US,en;q=0.9"
"referer"="https://analytics.amplitude.com/coinmara/settings/projects/386665/general"
"sec-ch-ua"="`"Not_A Brand`";v=`"99`", `"Microsoft Edge`";v=`"109`", `"Chromium`";v=`"109`""
"sec-ch-ua-mobile"="?0"
"sec-ch-ua-platform"="`"Windows`""
"sec-fetch-dest"="iframe"
"sec-fetch-mode"="navigate"
"sec-fetch-site"="same-origin"
"sec-fetch-user"="?1"
"upgrade-insecure-requests"="1"}


$url = "https://analytics.amplitude.com/api/2/export?start=$nextDate&end=$nextDate&downloadId=bTNEsn6"

Invoke-RestMethod  -Uri $url -Headers $header -Outfile $downloadFile
}

##Extract function
#This function extracts the app data from a single package to individual hourly gzip files
Function Extract-Amplitude{
    Param(
        $infile,
        $outfile = ($infile -replace '\.gz$','')
        )

    $finput = New-Object System.IO.FileStream $inFile, ([IO.FileMode]::Open), ([IO.FileAccess]::Read), ([IO.FileShare]::Read)
    $foutput = New-Object System.IO.FileStream $outFile, ([IO.FileMode]::Create), ([IO.FileAccess]::Write), ([IO.FileShare]::None)
    $gzipStream = New-Object System.IO.Compression.GzipStream $finput, ([IO.Compression.CompressionMode]::Decompress)

    $buffer = New-Object byte[](1024)
    while($true){
        $read = $gzipstream.Read($buffer, 0, 1024)
        if ($read -le 0){break}
        $foutput.Write($buffer, 0, $read)
        }

    $gzipStream.Close()
    $foutput.Close()
    $finput.Close()
}


###Parameters
##Amplitude Authorisation Taken
$authToken = "NDRjZjRmY2Y5ODBkN2FiODhmMGI0ODI0N2FiNjhhMzE6NGY2MjQxNTZhNWU0YzFjYWUxNWYzMGUyMzc1ZjE2OGE"
$projectCode = "386665"

##Database connection parameters
$hostname = "mara-bi-data-poc.cluster-ro-csvsrzen2vtn.eu-west-2.rds.amazonaws.com"
$dbname = "mara_bi_data_poc"
$username = "bi_write"
$password = "69WUdtRZKAxK2uKK"
$port = "5432"

##Extraction Duration
#Either use a static start and end date, or extract for previous day only
$startDate = "15-Nov-2022"
$startDate = [datetime]::parseexact($startDate, 'dd-MMM-yyyy', $null)
$startDate = (Get-Date).AddDays(-1) ###comment

$endDate = "30-Nov-2022"
$endDate = [datetime]::parseexact($endDate, 'dd-MMM-yyyy', $null)
$endDate = Get-Date ###comment

##Folder for storing the downloaded archives
$downloadPath = "/Users/victornneji/Documents/amplitude/downloads"

##Staging Table
$stagingTable = "bi_analytics.stg_amplitude_events"

##Cleaning Up and staging procedure
$stagingPrc = "bi_analytics.prc_amplitude_events()"





$arrayDate = 
   do {
       $startDate.ToString('yyyyMMdd')
       $startDate = $startDate.AddDays(1)
      } until ($startDate -gt $endDate)

foreach($date in $arrayDate) {
$nextDate = $date

$filename = "amplitude_"+$nextDate+".zip"
$downloadFile = "$downloadPath/$filename"
$extractPath = "$downloadPath/amplitude_"+$nextDate

###Test if the folder/file exists. If yes, then overwrite
if (Test-Path $downloadFile) {
    Remove-Item $downloadFile -verbose -Recurse
    Write-Host "Old File for $nextDate Deleted"
    Write-Host "New file for $nextDate will be created"
} else {
    Write-Host "New file for $nextDate will be created"
}

if (Test-Path $extractPath) {
    Remove-Item $extractPath -verbose -Recurse
    Write-Host "Old Extract Path for $nextDate Deleted"
    Write-Host "New Extract Path for $nextDate will be created"
} else {
    Write-Host "New Extract Path for $nextDate will be created"
}


##Execute Download and Extract Functions
Get-Amplitude -authKey $authToken -nextDate $nextDate -downloadFile  $downloadFile 
Expand-Archive -path $downloadFile -destinationpath $extractPath

###EXTRACT GZIP FILES###
$results = get-childItem "$extractPath/$projectCode/*.gz" 
foreach ($result in $results) {
    $filePath = $result 
    Extract-Amplitude($filePath)
}

###MOVE JSON FILES TO THEIR OWN FOLDER####
mkdir "$extractPath/json/" 
$results = get-childItem "$extractPath/$projectCode/*.json" 
foreach ($result in $results) {
    Move-Item -path $result -destination "$extractPath/json/" 
}


####CONVERT JSON FILES TO CSV#####
$results = get-childItem "$extractPath/json/" 
foreach ($result in $results) {

    $file = $result
    $fileName = $file.Name -replace('.json', '.csv')
    #Get-Content -Raw $file                                                                             
    $jsonb = Get-Content -Raw $file   
    $jsonb = $jsonb -creplace ("`n",",`n")        
    $jsonb = "["+$jsonb.substring(0, ($jsonb.length)-2)+"]"
    $jsoncsv = $jsonb|convertfrom-json

    $csvheaders = $jsoncsv|get-member -membertype NoteProperty 
    $Datatable = New-Object System.Data.DataTable

    Foreach ($header in $csvheaders ) {   

        $Datatable.Columns.Add($header.Name)
    
    }

    Foreach($csv in $jsoncsv) {
        $row = $Datatable.NewRow()
        Foreach ($header in $csvheaders.name)
        {   $row.$header = $csv.$header}
        $Datatable.Rows.Add($row)
    }

    $Datatable.Columns.Add("file_name")
    Foreach ($row in $Datatable)
    {
        $row."file_name" = $result.name
    }


    $Datatable|export-csv -path "$extractPath/json/$fileName" 
}

###MOVE CSV FILES TO THEIR OWN FOLDER####
mkdir "$extractPath/csv/" 
$results = get-childItem "$extractPath/json/*.csv" 
foreach ($result in $results) {
    Move-Item -path $result -destination "$extractPath/csv/" 
}


####LOAD TO REMOTE DATABASE####
$results = get-childItem "$extractPath/csv/" 
$command = "truncate table $stagingTable"
psql -d "postgresql://$username`:$password@$hostname`:$port/$dbname" -c $command

foreach ($result in $results) {
    
    
    $command = "\copy $stagingTable from '$result' with delimiter as ',' CSV HEADER"
    psql -d "postgresql://$username`:$password@$hostname`:$port/$dbname" -c $command
}

$command = "call $stagingPrc"
psql -d "postgresql://$username`:$password@$hostname`:$port/$dbname" -c $command

Write-Host "$nextDate completed"
}
