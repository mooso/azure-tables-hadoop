Param (
  [Parameter(Mandatory=$true)]
  [String]
  $ClusterName,
  [Parameter(Mandatory=$true)]
  [String]
  $ClusterPassword,
  [String]
  $StorageAccountName = $(Get-AzureSubscription -Current).CurrentStorageAccountName,
  [String]
  $ClusterUserName = 'Admin',
  [Int]
  $ClusterSize = 4,
  [String]
  $LibsContainerName = 'hiveontableslib'
)

$ErrorActionPreference = 'Stop'

# Find out where I am
$Invocation = (Get-Variable MyInvocation).Value
$ScriptLocation = Split-Path $Invocation.MyCommand.Path

# Figure out where the jar is
$JarPath = $(Get-ChildItem "$ScriptLocation\..\..\target\microsoft-hadoop-azure*.jar").FullName

# Error out if the jar isn't there
If ($JarPath -eq $null)
{
    Throw 'Jar not found. Make sure to "mvn package" first.'
}

# Get the key and create the context for the storage account
$AccountKey = $(Get-AzureStorageKey $StorageAccountName).Primary
$AccountContext = New-AzureStorageContext -StorageAccountName $StorageAccountName `
 -StorageAccountKey $AccountKey

# Create the libs container if not there already
$LibsContainer = Get-AzureStorageContainer $LibsContainerName -Context $AccountContext `
 -ErrorAction SilentlyContinue
If ($LibsContainer -eq $null)
{
    $LibsContainer = New-AzureStorageContainer -Context $AccountContext -Name $LibsContainerName
}

# Delete any blobs already there so we don't have version conflicts (HDInsight loads old code from old jars)
Get-AzureStorageBlob -Container $LibsContainerName -Context $AccountContext |
 Remove-AzureStorageBlob

# Upload the new jar
$JarBlob = Set-AzureStorageBlobContent -Container $LibsContainerName -Context $AccountContext `
 -Blob $(Split-Path $JarPath -Leaf) -File $JarPath

# Configure the cluster
$Location = $(Get-AzureStorageAccount $StorageAccountName).Location
$ClusterConfiguration = New-AzureHDInsightClusterConfig -ClusterSizeInNodes $ClusterSize
$ClusterConfiguration = Set-AzureHDInsightDefaultStorage $ClusterConfiguration `
 -StorageAccountName $StorageAccountName `
 -StorageAccountKey $AccountKey `
 -StorageContainerName $ClusterName
$ClusterCredentials = New-Object System.Management.Automation.PSCredential $ClusterUserName, `
 ($ClusterPassword | ConvertTo-SecureString -force -asplaintext)

$HiveConfiguration = New-Object $('Microsoft.WindowsAzure.Management.' + `
 'HDInsight.Cmdlet.DataObjects.AzureHDInsightHiveConfiguration')
$HiveConfiguration.AdditionalLibraries = New-Object $('Microsoft.WindowsAzure' + `
 '.Management.HDInsight.Cmdlet.DataObjects.AzureHDInsightDefaultStorageAccount')
$HiveConfiguration.AdditionalLibraries.StorageAccountName = "$StorageAccountName.blob.core.windows.net"
$HiveConfiguration.AdditionalLibraries.StorageAccountKey = $AccountKey
$HiveConfiguration.AdditionalLibraries.StorageContainerName = $LibsContainerName

$ClusterConfiguration = Add-AzureHDInsightConfigValues $ClusterConfiguration `
 -Hive $HiveConfiguration

# Create it
$Cluster = New-AzureHDInsightCluster -Config $ClusterConfiguration -Credential $ClusterCredentials `
 -Name $ClusterName -Location $Location
