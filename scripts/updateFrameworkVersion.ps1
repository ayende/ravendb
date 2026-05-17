param($Version)

function UpdateRootDirectoryBuildProps ( $path, $version ) {
    $versionPattern = [regex]'(?sm)<RuntimeFrameworkVersion>[A-Za-z0-9-\.\r\n\s]*</RuntimeFrameworkVersion>'
    $result = [System.IO.File]::ReadAllText($path)
    $result = $versionPattern.Replace($result, "<RuntimeFrameworkVersion>$version</RuntimeFrameworkVersion>")
    [System.IO.File]::WriteAllText($path, $result, [System.Text.Encoding]::UTF8)
}

function UpdateServerOptions ( $serverOptionsFile, $version ) {
    $versionPattern = [regex]'(?sm)public string FrameworkVersion { get; set; } = "[A-Za-z0-9-\.\r\n\s\+]*";'
    $result = [System.IO.File]::ReadAllText($serverOptionsFile)
    $result = $versionPattern.Replace($result, "public string FrameworkVersion { get; set; } = ""$version+"";")
    [System.IO.File]::WriteAllText($serverOptionsFile, $result, [System.Text.Encoding]::UTF8)
}

if ([string]::IsNullOrEmpty($Version)) {
    throw "Version is required."
}

$rootProps = (Get-ChildItem "Directory.Build.props").FullName
write-host "Update RuntimeFrameworkVersion in $rootProps"
UpdateRootDirectoryBuildProps $rootProps $Version

write-host "Update FrameworkVersion in ServerOptions.cs"
UpdateServerOptions $(Get-ChildItem ".\src\Raven.Embedded\ServerOptions.cs").FullName $Version
