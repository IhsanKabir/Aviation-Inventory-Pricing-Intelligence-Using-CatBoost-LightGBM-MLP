param(
    [Parameter(Mandatory = $true)]
    [string]$InputXlsx,
    [string]$OutputXlsm = ""
)

$ErrorActionPreference = "Stop"

function Resolve-FullPath([string]$PathValue) {
    if ([System.IO.Path]::IsPathRooted($PathValue)) {
        return [System.IO.Path]::GetFullPath($PathValue)
    }
    return [System.IO.Path]::GetFullPath((Join-Path (Get-Location) $PathValue))
}

$inPath = Resolve-FullPath $InputXlsx
if (-not (Test-Path -LiteralPath $inPath)) {
    throw "Input workbook not found: $inPath"
}

if ([string]::IsNullOrWhiteSpace($OutputXlsm)) {
    $OutputXlsm = [System.IO.Path]::ChangeExtension($inPath, ".xlsm")
}
$outPath = Resolve-FullPath $OutputXlsm

$excel = $null
$wb = $null
try {
    $excel = New-Object -ComObject Excel.Application
    $excel.Visible = $false
    $excel.DisplayAlerts = $false

    $wb = $excel.Workbooks.Open($inPath)
    # 52 = xlOpenXMLWorkbookMacroEnabled
    $wb.SaveAs($outPath, 52)

    $wsData = $null
    try {
        $wsData = $wb.Worksheets.Item("Route Filter View")
    } catch {
        throw "Worksheet 'Route Filter View' not found. Generate latest route monitor workbook first."
    }
    $wsMain = $null
    try {
        $wsMain = $wb.Worksheets.Item("Route Flight Fare Monitor")
    } catch {
        throw "Worksheet 'Route Flight Fare Monitor' not found. Generate latest route monitor workbook first."
    }
    $wsBlockIndex = $null
    try {
        $wsBlockIndex = $wb.Worksheets.Item("Route Block Index")
    } catch {
        throw "Worksheet 'Route Block Index' not found. Regenerate route monitor with latest output writer changes."
    }

    $wsCtl = $null
    try {
        $wsCtl = $wb.Worksheets.Item("Macro Control")
    } catch {
        $wsCtl = $wb.Worksheets.Add()
        $wsCtl.Name = "Macro Control"
    }

    $wsCtl.Cells.Item(1, 1).Value2 = "Route Monitor Macro Controls"
    $wsCtl.Cells.Item(2, 1).Value2 = "Airlines CSV"
    $wsCtl.Cells.Item(2, 2).Value2 = "BG,BS,2A"
    $wsCtl.Cells.Item(3, 1).Value2 = "Signals CSV"
    $wsCtl.Cells.Item(3, 2).Value2 = "INCREASE,DECREASE,NEW,SOLD OUT,UNKNOWN"
    $wsCtl.Cells.Item(5, 1).Value2 = "Use buttons below, or run macros:"
    $wsCtl.Cells.Item(6, 1).Value2 = "ApplyRouteFilters"
    $wsCtl.Cells.Item(7, 1).Value2 = "ClearRouteFilters"
    $wsCtl.Columns.Item("A:B").AutoFit() | Out-Null

    $vba = @"
Option Explicit

Private Function ParseCsv(ByVal raw As String) As Variant
    Dim txt As String
    txt = Trim(UCase(raw))
    If Len(txt) = 0 Then
        ParseCsv = Empty
        Exit Function
    End If
    Dim arr0() As String
    arr0 = Split(txt, ",")
    Dim tmp() As String
    ReDim tmp(0 To UBound(arr0))
    Dim i As Long, n As Long
    n = -1
    For i = LBound(arr0) To UBound(arr0)
        Dim v As String
        v = Trim(arr0(i))
        If Len(v) > 0 Then
            n = n + 1
            tmp(n) = v
        End If
    Next i
    If n < 0 Then
        ParseCsv = Empty
        Exit Function
    End If
    ReDim Preserve tmp(0 To n)
    ParseCsv = tmp
End Function

Private Function FindHeaderColumn(ByVal ws As Worksheet, ByVal headerRow As Long, ByVal headerName As String) As Long
    Dim lastCol As Long
    lastCol = ws.Cells(headerRow, ws.Columns.Count).End(xlToLeft).Column
    Dim c As Long
    For c = 1 To lastCol
        If UCase(Trim(CStr(ws.Cells(headerRow, c).Value2))) = UCase(Trim(headerName)) Then
            FindHeaderColumn = c
            Exit Function
        End If
    Next c
    FindHeaderColumn = 0
End Function

Private Function CollectionContains(ByVal coll As Collection, ByVal token As String) As Boolean
    If coll Is Nothing Then Exit Function
    Dim item As Variant
    For Each item In coll
        If UCase(CStr(item)) = UCase(token) Then
            CollectionContains = True
            Exit Function
        End If
    Next item
End Function

Private Function CsvIntersectsSelection(ByVal csvText As String, ByVal selected As Collection) As Boolean
    If selected Is Nothing Or selected.Count = 0 Then
        CsvIntersectsSelection = True
        Exit Function
    End If
    Dim arr() As String
    arr = Split(UCase(CStr(csvText)), ",")
    Dim i As Long, token As String
    For i = LBound(arr) To UBound(arr)
        token = Trim(arr(i))
        If Len(token) > 0 Then
            If CollectionContains(selected, token) Then
                CsvIntersectsSelection = True
                Exit Function
            End If
        End If
    Next i
    CsvIntersectsSelection = False
End Function

Private Function SelectedFromChecks(ByVal ws As Worksheet, ByVal prefix As String) As Collection
    Dim out As New Collection
    Dim cb As Object
    For Each cb In ws.CheckBoxes
        If LCase(Left(CStr(cb.Name), Len(prefix))) = LCase(prefix) Then
            If cb.Value = 1 Then
                On Error Resume Next
                out.Add UCase(Trim(CStr(cb.Caption))), UCase(Trim(CStr(cb.Caption)))
                On Error GoTo 0
            End If
        End If
    Next cb
    Set SelectedFromChecks = out
End Function

Public Sub ApplyRouteFilters()
    Dim wsData As Worksheet, wsCtl As Worksheet
    Set wsData = ThisWorkbook.Worksheets("Route Filter View")
    Set wsCtl = ThisWorkbook.Worksheets("Macro Control")

    Dim headerRow As Long
    headerRow = 7

    Dim lastRow As Long, lastCol As Long
    lastRow = wsData.Cells(wsData.Rows.Count, 1).End(xlUp).Row
    lastCol = wsData.Cells(headerRow, wsData.Columns.Count).End(xlToLeft).Column
    If lastRow <= headerRow Then Exit Sub

    Dim rng As Range
    Set rng = wsData.Range(wsData.Cells(headerRow, 1), wsData.Cells(lastRow, lastCol))

    If wsData.AutoFilterMode Then wsData.AutoFilterMode = False
    rng.AutoFilter

    Dim airlineCol As Long, signalCol As Long
    airlineCol = FindHeaderColumn(wsData, headerRow, "airline")
    signalCol = FindHeaderColumn(wsData, headerRow, "signal_primary")

    Dim arr As Variant
    arr = ParseCsv(CStr(wsCtl.Range("B2").Value2))
    If Not IsEmpty(arr) And airlineCol > 0 Then
        rng.AutoFilter Field:=airlineCol, Criteria1:=arr, Operator:=xlFilterValues
    End If

    arr = ParseCsv(CStr(wsCtl.Range("B3").Value2))
    If Not IsEmpty(arr) And signalCol > 0 Then
        rng.AutoFilter Field:=signalCol, Criteria1:=arr, Operator:=xlFilterValues
    End If

    wsData.Activate
End Sub

Public Sub ClearRouteFilters()
    Dim wsData As Worksheet
    Set wsData = ThisWorkbook.Worksheets("Route Filter View")
    If wsData.FilterMode Then wsData.ShowAllData
    wsData.AutoFilterMode = False
    wsData.Activate
End Sub

Public Sub ApplyMainSheetFilters()
    On Error GoTo EH

    Dim wsMain As Worksheet, wsIdx As Worksheet
    Set wsMain = ThisWorkbook.Worksheets("Route Flight Fare Monitor")
    Set wsIdx = ThisWorkbook.Worksheets("Route Block Index")

    Dim selAir As Collection, selSig As Collection
    Set selAir = SelectedFromChecks(wsMain, "mflt_air_")
    Set selSig = SelectedFromChecks(wsMain, "mflt_sig_")

    wsMain.Rows.Hidden = False
    wsMain.Rows("1:4").Hidden = False

    Dim lastRow As Long
    lastRow = wsIdx.Cells(wsIdx.Rows.Count, 1).End(xlUp).Row
    Dim r As Long
    For r = 2 To lastRow
        Dim startRow As Long, endRow As Long
        startRow = CLng(Val(wsIdx.Cells(r, 2).Value2))
        endRow = CLng(Val(wsIdx.Cells(r, 3).Value2))
        If startRow <= 0 Or endRow < startRow Then GoTo NextRow

        Dim airlinesCsv As String, signalsCsv As String
        airlinesCsv = CStr(wsIdx.Cells(r, 4).Value2)
        signalsCsv = CStr(wsIdx.Cells(r, 5).Value2)

        Dim keepBlock As Boolean
        keepBlock = CsvIntersectsSelection(airlinesCsv, selAir) And CsvIntersectsSelection(signalsCsv, selSig)
        wsMain.Rows(CStr(startRow) & ":" & CStr(endRow)).Hidden = Not keepBlock
NextRow:
    Next r

    wsMain.Activate
    Exit Sub
EH:
    MsgBox "ApplyMainSheetFilters failed: " & Err.Description, vbExclamation
End Sub

Public Sub ClearMainSheetFilters()
    Dim wsMain As Worksheet
    Set wsMain = ThisWorkbook.Worksheets("Route Flight Fare Monitor")

    Dim cb As Object
    For Each cb In wsMain.CheckBoxes
        If LCase(Left(CStr(cb.Name), 9)) = "mflt_air_" Or LCase(Left(CStr(cb.Name), 9)) = "mflt_sig_" Then
            cb.Value = 1
        End If
    Next cb

    wsMain.Rows.Hidden = False
    wsMain.Activate
End Sub
"@

    try {
        $vbProj = $wb.VBProject
        foreach ($comp in @($vbProj.VBComponents)) {
            if ($comp.Name -eq "RouteMonitorFilters") {
                $vbProj.VBComponents.Remove($comp)
                break
            }
        }
        # 1 = vbext_ct_StdModule
        $vbComp = $vbProj.VBComponents.Add(1)
        $vbComp.Name = "RouteMonitorFilters"
        $vbComp.CodeModule.AddFromString($vba) | Out-Null
    } catch {
        throw "VBA injection failed. Enable Excel setting: Trust Center > Macro Settings > Trust access to the VBA project object model."
    }

    foreach ($shape in @($wsCtl.Shapes)) {
        if ($shape.Name -eq "btnApplyFilters" -or $shape.Name -eq "btnClearFilters") {
            $shape.Delete()
        }
    }

    $btn1 = $wsCtl.Shapes.AddShape(1, 20, 150, 170, 28)
    $btn1.Name = "btnApplyFilters"
    $btn1.TextFrame.Characters().Text = "Apply Route Filters"
    $btn1.OnAction = "ApplyRouteFilters"

    $btn2 = $wsCtl.Shapes.AddShape(1, 210, 150, 170, 28)
    $btn2.Name = "btnClearFilters"
    $btn2.TextFrame.Characters().Text = "Clear Route Filters"
    $btn2.OnAction = "ClearRouteFilters"

    # In-sheet interactive controls on current monitor tab.
    $airlineCodes = @()
    for ($c = 2; $c -le 250; $c++) {
        $v = $wsMain.Cells.Item(2, $c).Value2
        if ($null -eq $v) { break }
        $sv = [string]$v
        if ([string]::IsNullOrWhiteSpace($sv)) { break }
        $airlineCodes += $sv.Trim().ToUpper()
    }
    if ($airlineCodes.Count -eq 0) {
        $airlineCodes = @("BG", "BS", "2A")
    }
    $signalCodes = @("INCREASE", "DECREASE", "NEW", "SOLD OUT", "UNKNOWN")

    $shapeNamesMain = @()
    foreach ($shape in @($wsMain.Shapes)) {
        $n = [string]$shape.Name
        if (
            $n -eq "btnApplyMainFilters" -or
            $n -eq "btnClearMainFilters" -or
            $n -like "mflt_air_*" -or
            $n -like "mflt_sig_*"
        ) {
            $shapeNamesMain += $n
        }
    }
    foreach ($n in $shapeNamesMain) {
        try { $wsMain.Shapes.Item($n).Delete() } catch {}
    }

    $anchorCol = 28
    $baseLeft = [double]$wsMain.Cells.Item(1, $anchorCol).Left
    $baseTop = [double]$wsMain.Cells.Item(1, $anchorCol).Top
    $wsMain.Cells.Item(1, $anchorCol).Value2 = "Interactive Filters (Current Sheet)"
    $wsMain.Cells.Item(2, $anchorCol).Value2 = "Airlines:"

    $perRow = 6
    $cbW = 70
    $cbH = 16
    $xGap = 74
    $yGap = 18

    for ($i = 0; $i -lt $airlineCodes.Count; $i++) {
        $code = [string]$airlineCodes[$i]
        $r = [int][Math]::Floor($i / $perRow)
        $k = [int]($i % $perRow)
        $left = $baseLeft + ($k * $xGap)
        $top = $baseTop + 20 + ($r * $yGap)
        $cb = $wsMain.CheckBoxes().Add($left, $top, $cbW, $cbH)
        $cb.Caption = $code
        $cb.Name = "mflt_air_$code"
        $cb.Value = 1
        $cb.OnAction = "ApplyMainSheetFilters"
    }

    $airRows = [int][Math]::Ceiling([double]$airlineCodes.Count / [double]$perRow)
    if ($airRows -lt 1) { $airRows = 1 }
    $signalTop = $baseTop + 20 + (($airRows + 1) * $yGap)
    $wsMain.Cells.Item(2 + $airRows + 1, $anchorCol).Value2 = "Signals:"

    for ($i = 0; $i -lt $signalCodes.Count; $i++) {
        $code = [string]$signalCodes[$i]
        $r = [int][Math]::Floor($i / $perRow)
        $k = [int]($i % $perRow)
        $left = $baseLeft + ($k * $xGap)
        $top = $signalTop + ($r * $yGap)
        $codeName = $code.Replace(" ", "_")
        $cb = $wsMain.CheckBoxes().Add($left, $top, $cbW + 8, $cbH)
        $cb.Caption = $code
        $cb.Name = "mflt_sig_$codeName"
        $cb.Value = 1
        $cb.OnAction = "ApplyMainSheetFilters"
    }

    $sigRows = [int][Math]::Ceiling([double]$signalCodes.Count / [double]$perRow)
    if ($sigRows -lt 1) { $sigRows = 1 }
    $btnTop = $signalTop + ($sigRows * $yGap) + 6

    $wsMain.Cells.Item(2 + $airRows + $sigRows + 3, $anchorCol).Value2 = "Click any checkbox to apply instantly."

    $btnMainB = $wsMain.Shapes.AddShape(1, $baseLeft, $btnTop, 200, 24)
    $btnMainB.Name = "btnClearMainFilters"
    $btnMainB.TextFrame.Characters().Text = "Clear Main Sheet Filters"
    $btnMainB.OnAction = "ClearMainSheetFilters"

    $wb.Save()
    Write-Output "xlsm_exported=$outPath"
} finally {
    if ($wb -ne $null) { $wb.Close($true) | Out-Null }
    if ($excel -ne $null) {
        $excel.Quit() | Out-Null
        [System.Runtime.InteropServices.Marshal]::ReleaseComObject($excel) | Out-Null
    }
}
