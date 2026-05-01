[CmdletBinding()]
param()

Push-Location (Resolve-Path "$PSScriptRoot\..")
try {
    dotnet run --project .\KafkaBenchmarks.csproj -c Release
} finally {
    Pop-Location
}