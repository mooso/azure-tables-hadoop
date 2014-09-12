$ErrorActionPreference = 'Stop'

# Find out where I am
$Invocation = (Get-Variable MyInvocation).Value
$ScriptLocation = Split-Path $Invocation.MyCommand.Path

# Figure out where the jar is
$JarPath = $(Get-ChildItem "$ScriptLocation\..\..\target\microsoft-hadoop-azure*.jar").FullName

# Copy it over to the Hive lib directory
copy $JarPath "$env:HIVE_HOME\lib"
copy $JarPath "$env:HADOOP_HOME\share\hadoop\common"

# Restart Hive Server 2 service
$HiveService = Get-Service hiveserver2 -ErrorAction SilentlyContinue
if ($HiveService -ne $null)
{
    Restart-Service $HiveService
}
