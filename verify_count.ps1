
$content = Get-Content 'C:/Users/ccrossley/Downloads/dist61.geojson' -Raw
Write-Host "File length: $($content.Length)"
$pairs = [regex]::Matches($content, '\[(-\d+\.?\d*),(\d+\.?\d*)\]')
Write-Host "Coord pairs matched: $($pairs.Count)"
# Show first and last few pairs for sanity check
Write-Host "First pair: $($pairs[0].Value)"
Write-Host "Last pair: $($pairs[$pairs.Count-1].Value)"
# Show a sample around index 85-90 to confirm the 40.267 region
for ($i = 82; $i -le 92; $i++) {
    if ($i -lt $pairs.Count) {
        Write-Host "idx=$i  $($pairs[$i].Value)"
    }
}
