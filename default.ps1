Include ".\build_utils.ps1"

properties {
	$base_dir  = resolve-path .
	$lib_dir = "$base_dir\SharedLibs"
	$ravendb_dir = "$base_dir\RavenDB"
	$output_dir = "$base_dir\Output"
	$packages_dir = "$base_dir\packages"
	$sln_file = "$base_dir\zzz_RavenDB_Release.sln"
	$version = "3.0"
	$tools_dir = "$base_dir\Tools"
	$release_dir = "$base_dir\Release"
	$uploader = "..\Uploader\S3Uploader.exe"
	$global:configuration = "Release"
	$signTool = "C:\Program Files (x86)\Windows Kits\8.0\bin\x86\signtool.exe"
	$installerCert = "..\certs\installer.pfx"
	$certPassword = $null
	
	$core_db_dlls = @(
        "Raven.Abstractions.???", 
        (Get-DependencyPackageFiles 'NLog.2'), 
        (Get-DependencyPackageFiles Microsoft.Web.Infrastructure), 
        "Jint.Raven.???", "Lucene.Net.???", "Microsoft.Data.Edm.???", "Microsoft.WindowsAzure.Storage.???",
		"Microsoft.Data.OData.???", "Microsoft.WindowsAzure.ConfigurationManager.???", "Lucene.Net.Contrib.Spatial.NTS.???", 
		"Spatial4n.Core.NTS.???", "GeoAPI.dll", "NetTopologySuite.dll", "PowerCollections.dll", 
		"ICSharpCode.NRefactory.???", "ICSharpCode.NRefactory.CSharp.???", "Mono.Cecil.???", "Esent.Interop.???", 
		"Raven.Database.???", "AWS.Extensions.???", "AWSSDK.???"  )

	$backup_files = @( "Raven.Abstractions.???", "Raven.Backup.???" ) 
   
   	$bundles = @("Raven.Bundles.Authorization", "Raven.Bundles.CascadeDelete", 
   		    	 "Raven.Bundles.UniqueConstraints","Raven.Client.Authorization", 
   		    	 "Raven.Client.UniqueConstraints")
	
	$web_dlls = ( @( "Raven.Web.???"  ) + $core_db_dlls)
	
	$web_files = @("RavenDB\Shared\DefaultConfigs\web.config", "RavenDB\Shared\DefaultConfigs\NLog.Ignored.config" )

	$server_exe = ( @( "Raven.Server.???") + $core_db_dlls )

	$server_files = @("RavenDB\Shared\DefaultConfigs\NLog.Ignored.config")
		
	$client_dlls = @( (Get-DependencyPackageFiles 'NLog.2'), "Raven.Client.MvcIntegration.???", 
					"Raven.Abstractions.???", 
					"Raven.Client.Lightweight.???") 
		
	$silverlight_dlls = @("Raven.Client.Silverlight.???", 
		"AsyncCtpLibrary_Silverlight5.???", 
		"Microsoft.CompilerServices.AsyncTargetingPack.Silverlight5.???") 
 
	$all_client_dlls = ( @( "Raven.Client.Embedded.???") + $client_dlls + $core_db_dlls )
	  
	$test_prjs = @("Tests\Raven.Tests","Bundles\Raven.Bundles.Tests" )
}

task default -depends Stable,Release

task Verify40 {
	if( (ls "$env:windir\Microsoft.NET\Framework\v4.0*") -eq $null ) {
		throw "Building Raven requires .NET 4.0, which doesn't appear to be installed on this machine"
	}
}

task Clean {
	# Remove-Item -force -recurse $buildartifacts_dir -ErrorAction SilentlyContinue
	Remove-Item -force -recurse $release_dir -ErrorAction SilentlyContinue
}

task Init -depends Verify40, Clean {

	if($env:BUILD_NUMBER -ne $null) {
		$env:buildlabel  = $env:BUILD_NUMBER
	}
	if($env:buildlabel -eq $null) {
		$env:buildlabel = "13"
	}
	
	$commit = Get-Git-Commit
	(Get-Content "$base_dir\RavenDB\CommonAssemblyInfo.cs") | 
		Foreach-Object { $_ -replace ".13", ".$($env:buildlabel)" } |
		Foreach-Object { $_ -replace "{commit}", $commit } |
		Set-Content "$base_dir\RavenDB\CommonAssemblyInfo.cs" -Encoding UTF8
	
	New-Item $release_dir -itemType directory -ErrorAction SilentlyContinue | Out-Null
	New-Item $ravendb_dir -itemType directory -ErrorAction SilentlyContinue | Out-Null
}

task Compile -depends Init {

	"Dummy file so msbuild knows there is one here before embedding as resource." | Out-File "$base_dir\RavenDB\Server\Raven.Database\Server\WebUI\Raven.Studio.xap"
	
	$v4_net_version = (ls "$env:windir\Microsoft.NET\Framework\v4.0*").Name
	
	$dat = "$base_dir\..\BuildsInfo\RavenDB\Settings.dat"
	$datDest = "$base_dir\RavenDB\Clients\Raven.Studio\Settings.dat"
	echo $dat
	if (Test-Path $dat) {
		Copy-Item $dat $datDest -force
	}
	ElseIf ((Test-Path $datDest) -eq $false) {
		New-Item $datDest -type file -force
	}
	
	Write-Host "Compiling with '$global:configuration' configuration" -ForegroundColor Yellow
	exec { &"C:\Windows\Microsoft.NET\Framework\$v4_net_version\MSBuild.exe" "$sln_file" /p:Configuration=$global:configuration /p:nowarn="1591 1573" }
	remove-item "$base_dir\nlog.config" -force  -ErrorAction SilentlyContinue
}

task FullStorageTest {
	$global:full_storage_test = $true
}

task Test  {
	Clear-Host
	
	Write-Host $test_prjs
	
	$xUnit = Get-PackagePath xunit.runners.1.9
	$xUnit = "$xUnit\tools\xunit.console.clr4.exe"
	Write-Host "xUnit location: $xUnit"
	
	$test_prjs | ForEach-Object { 
		if($global:full_storage_test) {
			$env:raventest_storage_engine = 'esent';
			Write-Host "Testing $base_dir\$_ (esent)"
		}
		else {
			$env:raventest_storage_engine = $null;
			Write-Host "Testing $base_dir\$_ (default)"
		}
		$test = [System.IO.Path]::GetFileName($_);
		$path = "$base_dir\RavenDB\$_\bin\$global:configuration"
		Push-Location $path 
		Try {
			exec { &"$xUnit" "$test.dll" }
		}
		Finally {
			Pop-Location
		}
	}
}

task StressTest -depends Compile {
	
	$xUnit = Get-PackagePath xunit.runners
	$xUnit = "$xUnit\tools\xunit.console.clr4.exe"
	
	@("Raven.StressTests.dll") | ForEach-Object { 
		Write-Host "Testing $base_dir\$_"
		
		if($global:full_storage_test) {
			$env:raventest_storage_engine = 'esent';
			Write-Host "Testing $base_dir\$_ (esent)"
			&"$xUnit" "$base_dir\$_"
		}
		else {
			$env:raventest_storage_engine = $null;
			Write-Host "Testing $base_dir\$_ (default)"
			&"$xUnit" "$base_dir\$_"
		}
	}
}

task MeasurePerformance -depends Compile {
	$RavenDbStableLocation = "F:\RavenDB"
	$DataLocation = "F:\Data"
	$LogsLocation = "F:\PerformanceLogs"
	$stableBuildToTests = @(616, 573, 531, 499, 482, 457, 371)
	$stableBuildToTests | ForEach-Object { 
		$RavenServer = $RavenDbStableLocation + "\RavenDB-Build-$_\Server"
		Write-Host "Measure performance against RavenDB Build #$_, Path: $RavenServer"
		exec { &"$base_dir\Raven.Performance.exe" "--database-location=$RavenDbStableLocation --build-number=$_ --data-location=$DataLocation --logs-location=$LogsLocation" }
	}
}

task TestSilverlight -depends Compile, CopyServer {
	try
	{
		$process = Start-Process "$base_dir\Output\Server\Raven.Server.exe" "--ram --set=Raven/Port==8079" -PassThru
	
		$statLight = Get-PackagePath StatLight
		$statLight = "$statLight\tools\StatLight.exe"
		&$statLight "--XapPath=.\bin\release\sl5\Raven.Tests.Silverlight.xap" "--OverrideTestProvider=MSTestWithCustomProvider" "--ReportOutputFile=.\bin\release\sl5\Raven.Tests.Silverlight.Results.xml" 
	}
	finally
	{
		Stop-Process -InputObject $process
	}
}

task TestWinRT -depends Compile, CopyServer {
	try
	{
		exec { CheckNetIsolation LoopbackExempt -a -n=68089da0-d0b7-4a09-97f5-30a1e8f9f718_pjnejtz0hgswm }
		
		$process = Start-Process "$base_dir\Output\Server\Raven.Server.exe" "--ram --set=Raven/Port==8079" -PassThru
	
		$testRunner = "C:\Program Files (x86)\Microsoft Visual Studio 11.0\Common7\IDE\CommonExtensions\Microsoft\TestWindow\vstest.console.exe"
	
		@("Raven.Tests.WinRT.dll") | ForEach-Object { 
			Write-Host "Testing $base_dir\winrt\$_"
			
			if($global:full_storage_test) {
				$env:raventest_storage_engine = 'esent';
				Write-Host "Testing $base_dir\winrt\$_ (esent)"
				&"$testRunner" "$base_dir\winrt\$_"
			}
			else {
				$env:raventest_storage_engine = $null;
				Write-Host "Testing $base_dir\winrt\$_ (default)"
				&"$testRunner" "$base_dir\winrt\$_"
			}
		}
	}
	finally
	{
		Stop-Process -InputObject $process
	}
}

task ReleaseNoTests -depends Stable,DoRelease {

}

task Vnext3 {
	$global:uploadCategory = "RavenDB-Unstable"
	$global:uploadMode = "Vnext3"
}

task Unstable {
	$global:uploadCategory = "RavenDB-Unstable"
	$global:uploadMode = "Unstable"
}

task Stable {
	$global:uploadCategory = "RavenDB"
	$global:uploadMode = "Stable"
}

task RunTests -depends Test,TestSilverlight,TestWinRT

task RunAllTests -depends FullStorageTest,RunTests,StressTest

task Release -depends RunTests,DoRelease

task CreateOutputDirectories -depends CleanOutputDirectory {
	New-Item $output_dir -Type directory -ErrorAction SilentlyContinue | Out-Null
	New-Item $output_dir\Server -Type directory | Out-Null
	New-Item $output_dir\Web -Type directory | Out-Null
	New-Item $output_dir\Web\bin -Type directory | Out-Null
	New-Item $output_dir\EmbeddedClient -Type directory | Out-Null
	New-Item $output_dir\Client -Type directory | Out-Null
	New-Item $output_dir\Silverlight -Type directory | Out-Null
	New-Item $output_dir\Bundles -Type directory | Out-Null
	New-Item $output_dir\Smuggler -Type directory | Out-Null
	New-Item $output_dir\Backup -Type directory | Out-Null
}

task CleanOutputDirectory { 
	Remove-Item $output_dir -Recurse -Force -ErrorAction SilentlyContinue
}

task CopyEmbeddedClient { 
	Copy-Project-Files -files $all_client_dlls `
	      -dest "Output\EmbeddedClient"  `
	      -project "RavenDB\Clients\Raven.Client.Embedded" 
}

task CopySilverlight { 
	$files = $silverlight_dlls + @((Get-DependencyPackageFiles 'NLog.2' -FrameworkVersion sl5)) 
	Copy-Project-Files -files $files `
	      -dest "Output\Silverlight"  `
	      -project "RavenDB\Clients\Raven.Client.Silverlight"
}

task CopySmuggler {
	Copy-Project-Files -files @("Raven.Abstractions.???", "Raven.Smuggler.???") `
	      -dest "Output\Smuggler"  `
	      -project "RavenDB\Clients\Raven.Smuggler" 
}

task CopyBackup {
	Copy-Project-Files -files $backup_files `
	      -dest "Output\Backup"  `
	      -project "RavenDB\Clients\Raven.Backup" 
}

task CopyClient {
	Copy-Project-Files -files $client_dlls `
	      -dest "Output\Client"  `
	      -project "RavenDB\Clients\Raven.Client.Lightweight" 
}

task CopyWeb {
	Copy-Files -files $web_dlls `
	      -dest "Output\Web\bin"  `
	      -path "RavenDB\Server\Raven.Web\bin\release" 

	Copy-Files -files $web_files `
	      -dest "Output\Web" `
	      -path $base_dir
}

task CopyBundles {

	foreach($bundle in $bundles) {
		Copy-Item "RavendB\Bundles\$bundle\bin\$global:configuration\$bundle.???" "Output\Bundles"

	}
	
}

task CopyServer {
	Copy-Project-Files -files $server_exe `
	      -dest "Output\Server"  `
	      -project "RavenDB\Server\Raven.Server" 

	Copy-Files -files $server_files `
	      -dest "Output\Server"  `
	      -path $base_dir

	Copy-Item $base_dir\RavenDB\Shared\DefaultConfigs\RavenDb.exe.config $base_dir\Output\Server\Raven.Server.exe.config
}

task CopyInstaller {
	if($env:buildlabel -eq 13)
	{
	  return
	}

	Copy-Item "$base_dir\RavenDB\Setup\Raven.Setup\bin\$global:configuration\RavenDB.Setup.exe" "$release_dir\$global:uploadCategory-Build-$env:buildlabel.Setup.exe"
}

task SignInstaller {
	if($env:buildlabel -eq 13)
	{
	  return
	}
	
	if (!(Test-Path $signTool)) 
	{
		throw "Could not find SignTool.exe under the specified path $signTool"
	}
	
	if (!(Test-Path $installerCert)) 
	{
		throw "Could not find pfx file under the path $installerCert to sign the installer"
	}
	
	if ($certPassword -eq $null) 
	{
		throw "Certificate password must be provided"
	}
	
	$installerFile = "$release_dir\$global:uploadCategory-Build-$env:buildlabel.Setup.exe"
		
	Exec { &$signTool sign /f "$installerCert" /p "$certPassword" /d "RavenDB" /du "http://ravendb.net" /t "http://timestamp.verisign.com/scripts/timstamp.dll" "$installerFile" }
}


task CreateDocs {
	$v4_net_version = (ls "$env:windir\Microsoft.NET\Framework\v4.0*").Name
	
	if($env:buildlabel -eq 13)
	{
	  return 
	}
	 
	# we expliclty allows this to fail
	#exec { &"C:\Windows\Microsoft.NET\Framework\$v4_net_version\MSBuild.exe" "$base_dir\RavenDB\Raven.Docs.shfbproj" /p:OutDir="$buildartifacts_dir\" }
}

task CopyRootFiles -depends CreateDocs {
	cp $base_dir\license.txt Output\license.txt
	cp $base_dir\RavenDB\Shared\Start.cmd Output\Start.cmd
	# cp $base_dir\Scripts\Raven-UpdateBundles.ps1 Output\Raven-UpdateBundles.ps1
	# cp $base_dir\Scripts\Raven-GetBundles.ps1 Output\Raven-GetBundles.ps1
	cp $base_dir\readme.txt Output\readme.txt
	cp $base_dir\Help\Documentation.chm Output\Documentation.chm  -ErrorAction SilentlyContinue
	cp $base_dir\RavenDB\acknowledgments.txt Output\acknowledgments.txt
}

task ZipOutput {

	if($env:buildlabel -eq 13)
	{
		return 
	}

	$old = pwd
	cd $output_dir
	
	$file = "$release_dir\$global:uploadCategory-Build-$env:buildlabel.zip"
		
	exec { 
		& $tools_dir\zip.exe -9 -A -r `
			$file `
			EmbeddedClient\*.* `
			Client\*.* `
			Smuggler\*.* `
			Backup\*.* `
			Web\*.* `
			Bundles\*.* `
			Web\bin\*.* `
			Server\*.* `
			*.*
	}
	
	cd $old
}

task ResetBuildArtifcats {
	
}

task DoRelease -depends Compile, `
	CleanOutputDirectory, `
	CreateOutputDirectories, `
	CopyEmbeddedClient, `
	CopySmuggler, `
	CopyBackup, `
	CopyClient, `
	CopySilverlight, `
	CopyWeb, `
	CopyBundles, `
	CopyServer, `
	CopyRootFiles, `
	ZipOutput, `
	CopyInstaller, `
	SignInstaller, `
	ResetBuildArtifcats {	
	Write-Host "Done building RavenDB"
}


task Upload -depends DoRelease {
	Write-Host "Starting upload"
	if (Test-Path $uploader) {
		$log = $env:push_msg 
		if(($log -eq $null) -or ($log.Length -eq 0)) {
		  $log = git log -n 1 --oneline		
		}
		
		$log = $log.Replace('"','''') # avoid problems because of " escaping the output
		
		$zipFile = "$release_dir\$global:uploadCategory-Build-$env:buildlabel.zip"
		$installerFile = "$release_dir\$global:uploadCategory-Build-$env:buildlabel.Setup.exe"
		
		$files = @(@($installerFile, $uploadCategory.Replace("RavenDB", "RavenDB Installer")) , @($zipFile, "$uploadCategory"))
		
		foreach ($obj in $files)
		{
			$file = $obj[0]
			$currentUploadCategory = $obj[1]
			write-host "Executing: $uploader ""$currentUploadCategory"" ""$env:buildlabel"" $file ""$log"""
			
			$uploadTryCount = 0
			while ($uploadTryCount -lt 5){
				$uploadTryCount += 1
				Exec { &$uploader "$currentUploadCategory" "$env:buildlabel" $file "$log" }
				
				if ($lastExitCode -ne 0) {
					write-host "Failed to upload to S3: $lastExitCode. UploadTryCount: $uploadTryCount"
				}
				else {
					break
				}
			}
			
			if ($lastExitCode -ne 0) {
				write-host "Failed to upload to S3: $lastExitCode. UploadTryCount: $uploadTryCount. Build will fail."
				throw "Error: Failed to publish build"
			}
		}
	}
	else {
		Write-Host "could not find upload script $uploadScript, skipping upload"
	}
}	

task UploadStable -depends Stable, DoRelease, Upload	

task UploadUnstable -depends Unstable, DoRelease, Upload

task UploadVnext3 -depends Vnext3, DoRelease, Upload

TaskTearDown {
	if ($LastExitCode -ne 0) {
		write-host "TaskTearDown detected an error. Build failed." -BackgroundColor Red -ForegroundColor Yellow
		write-host "Yes, something was failed!!!!!!!!!!!!!!!!!!!!!" -BackgroundColor Red -ForegroundColor Yellow
		# throw "TaskTearDown detected an error. Build failed."
		exit 1
	}
}
