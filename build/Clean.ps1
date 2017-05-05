$root_dir = Resolve-Path "$PSScriptRoot\..\"
Push-Location $root_dir

Get-ChildItem $root_dir\src -include bin,obj -Recurse | foreach ($_) { remove-item $_.fullname -Force -Recurse }
Get-ChildItem $root_dir\test -include bin,obj -Recurse | foreach ($_) { remove-item $_.fullname -Force -Recurse }