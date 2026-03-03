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
    $wsRowIndex = $null
    try {
        $wsRowIndex = $wb.Worksheets.Item("Route Row Index")
    } catch {
        throw "Worksheet 'Route Row Index' not found. Regenerate route monitor with latest output writer changes."
    }
    $wsColIndex = $null
    try {
        $wsColIndex = $wb.Worksheets.Item("Route Column Index")
    } catch {
        throw "Worksheet 'Route Column Index' not found. Regenerate route monitor with latest output writer changes."
    }
    $wsBase = $null
    try {
        $wsBase = $wb.Worksheets.Item("Route Monitor Base")
    } catch {
        $wsBase = $wb.Worksheets.Add()
        $wsBase.Name = "Route Monitor Base"
    }

    $wsCtl = $null
    try {
        $wsCtl = $wb.Worksheets.Item("Macro Control")
    } catch {
        $wsCtl = $wb.Worksheets.Add()
        $wsCtl.Name = "Macro Control"
    }

    $wsCtl.Cells.Item(1, 1).Value2 = "Route Monitor Macro Controls"
    $wsCtl.Cells.Item(2, 1).Value2 = "Airlines CSV (optional)"
    $wsCtl.Cells.Item(2, 2).Value2 = ""
    $wsCtl.Cells.Item(3, 1).Value2 = "Signals CSV (optional)"
    $wsCtl.Cells.Item(3, 2).Value2 = ""
    $wsCtl.Cells.Item(4, 1).Value2 = "Main Sheet Mode"
    $wsCtl.Cells.Item(4, 2).Value2 = "CONTEXT"
    $wsCtl.Cells.Item(5, 1).Value2 = "Main sheet is click-based. Use CSV fields only for Route Filter View."
    $wsCtl.Cells.Item(6, 1).Value2 = "ApplyRouteFilters"
    $wsCtl.Cells.Item(7, 1).Value2 = "ClearRouteFilters"
    $wsCtl.Columns.Item("A:B").AutoFit() | Out-Null
    $wsCtl.Visible = 2  # xlSheetVeryHidden

    $wsBase.Cells.Clear() | Out-Null
    $wsMain.UsedRange.Copy($wsBase.Range("A1")) | Out-Null
    $wsBase.Visible = 2  # xlSheetVeryHidden

    $vba = @"
Option Explicit

Private Const MODE_CONTEXT As String = "CONTEXT"
Private Const MODE_STRICT As String = "STRICT"

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

Private Sub AddUnique(ByVal coll As Collection, ByVal token As String)
    If Len(token) = 0 Then Exit Sub
    On Error Resume Next
    coll.Add token, token
    On Error GoTo 0
End Sub

Private Function CsvToCollection(ByVal raw As String) As Collection
    Dim out As New Collection
    Dim arr As Variant
    arr = ParseCsv(raw)
    If IsEmpty(arr) Then
        Set CsvToCollection = out
        Exit Function
    End If
    Dim i As Long
    For i = LBound(arr) To UBound(arr)
        AddUnique out, CStr(arr(i))
    Next i
    Set CsvToCollection = out
End Function

Private Function CollectionToCsv(ByVal coll As Collection) As String
    Dim txt As String
    Dim item As Variant
    For Each item In coll
        If Len(txt) > 0 Then txt = txt & ","
        txt = txt & UCase(CStr(item))
    Next item
    CollectionToCsv = txt
End Function

Private Function CloneCollection(ByVal src As Collection) As Collection
    Dim out As New Collection
    Dim item As Variant
    For Each item In src
        AddUnique out, UCase(CStr(item))
    Next item
    Set CloneCollection = out
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

Private Function CollectionEquals(ByVal a As Collection, ByVal b As Collection) As Boolean
    If a Is Nothing Or b Is Nothing Then Exit Function
    If a.Count <> b.Count Then Exit Function
    Dim item As Variant
    For Each item In a
        If Not CollectionContains(b, UCase(CStr(item))) Then Exit Function
    Next item
    CollectionEquals = True
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

Private Function NormalizeSignalToken(ByVal raw As String) As String
    Dim t As String
    t = UCase(Trim(raw))
    If Len(t) = 0 Then Exit Function
    If InStr(t, "INCREASE") > 0 Or InStr(t, "↑") > 0 Then
        NormalizeSignalToken = "INCREASE"
        Exit Function
    End If
    If InStr(t, "DECREASE") > 0 Or InStr(t, "↓") > 0 Then
        NormalizeSignalToken = "DECREASE"
        Exit Function
    End If
    If t = "NEW" Then
        NormalizeSignalToken = "NEW"
        Exit Function
    End If
    If InStr(t, "SOLD") > 0 Then
        NormalizeSignalToken = "SOLD OUT"
        Exit Function
    End If
    If InStr(t, "UNKNOWN") > 0 Or InStr(t, "—") > 0 Then
        NormalizeSignalToken = "UNKNOWN"
        Exit Function
    End If
    If t = "STABLE" Then
        NormalizeSignalToken = "UNKNOWN"
    End If
End Function

Private Function IsModeStrict(ByVal wsCtl As Worksheet) As Boolean
    Dim modeValue As String
    modeValue = UCase(Trim(CStr(wsCtl.Range("B4").Value2)))
    IsModeStrict = (modeValue = MODE_STRICT)
End Function

Private Sub RestoreMainSheetFromBase()
    Dim wsMain As Worksheet, wsBase As Worksheet
    Set wsMain = ThisWorkbook.Worksheets("Route Flight Fare Monitor")
    Set wsBase = ThisWorkbook.Worksheets("Route Monitor Base")

    Dim lr As Long, lc As Long
    lr = wsBase.Cells(wsBase.Rows.Count, 1).End(xlUp).Row
    lc = wsBase.Cells(1, wsBase.Columns.Count).End(xlToLeft).Column
    If lr < 1 Or lc < 1 Then Exit Sub
    wsBase.Range(wsBase.Cells(1, 1), wsBase.Cells(lr, lc)).Copy wsMain.Cells(1, 1)
End Sub

Private Function RowMatchesContext(ByVal rowAirCsv As String, ByVal rowSigCsv As String, ByVal selAir As Collection, ByVal selSig As Collection, ByVal airFilterActive As Boolean, ByVal sigFilterActive As Boolean) As Boolean
    If airFilterActive Then
        If Not CsvIntersectsSelection(rowAirCsv, selAir) Then Exit Function
    End If
    If sigFilterActive Then
        If Not CsvIntersectsSelection(rowSigCsv, selSig) Then Exit Function
    End If
    RowMatchesContext = True
End Function

Private Function RowMatchesStrict(ByVal rowAirCsv As String, ByVal rowSigCsv As String, ByVal airSigCsv As String, ByVal selAir As Collection, ByVal selSig As Collection, ByVal airFilterActive As Boolean, ByVal sigFilterActive As Boolean) As Boolean
    If airFilterActive Then
        If Not CsvIntersectsSelection(rowAirCsv, selAir) Then Exit Function
    End If
    If Not sigFilterActive Then
        RowMatchesStrict = True
        Exit Function
    End If
    If Len(Trim(airSigCsv)) = 0 Then
        RowMatchesStrict = CsvIntersectsSelection(rowSigCsv, selSig)
        Exit Function
    End If

    Dim pairs() As String
    pairs = Split(UCase(CStr(airSigCsv)), ";")
    Dim i As Long
    For i = LBound(pairs) To UBound(pairs)
        Dim token As String
        token = Trim(pairs(i))
        If Len(token) = 0 Then GoTo NextPair

        Dim pos As Long
        pos = InStr(1, token, ":", vbTextCompare)
        If pos <= 0 Then GoTo NextPair

        Dim ac As String
        ac = Trim(Left(token, pos - 1))
        If Len(ac) = 0 Then GoTo NextPair
        If airFilterActive And Not CollectionContains(selAir, ac) Then GoTo NextPair

        Dim sigPart As String
        sigPart = Mid(token, pos + 1)
        Dim sigArr() As String
        sigArr = Split(sigPart, "|")
        Dim j As Long
        For j = LBound(sigArr) To UBound(sigArr)
            Dim s As String
            s = NormalizeSignalToken(sigArr(j))
            If Len(s) > 0 Then
                If CollectionContains(selSig, s) Then
                    RowMatchesStrict = True
                    Exit Function
                End If
            End If
        Next j
NextPair:
    Next i
End Function

Private Sub ApplyStrictColumnMask(ByVal selAir As Collection, ByVal airFilterActive As Boolean, ByVal routeHasMatch As Object, ByVal routeDataMin As Object, ByVal routeDataMax As Object)
    If Not airFilterActive Then Exit Sub

    Dim wsMain As Worksheet, wsCol As Worksheet
    Set wsMain = ThisWorkbook.Worksheets("Route Flight Fare Monitor")
    Set wsCol = ThisWorkbook.Worksheets("Route Column Index")

    Dim lastRow As Long
    lastRow = wsCol.Cells(wsCol.Rows.Count, 1).End(xlUp).Row

    Dim r As Long
    For r = 2 To lastRow
        Dim routeKey As String
        routeKey = UCase(Trim(CStr(wsCol.Cells(r, 1).Value2)))
        If Len(routeKey) = 0 Then GoTo NextCol
        If Not routeHasMatch.Exists(routeKey) Then GoTo NextCol
        If Not CBool(routeHasMatch(routeKey)) Then GoTo NextCol

        Dim airline As String
        airline = UCase(Trim(CStr(wsCol.Cells(r, 4).Value2)))
        If Len(airline) = 0 Then GoTo NextCol
        If CollectionContains(selAir, airline) Then GoTo NextCol

        If Not routeDataMin.Exists(routeKey) Or Not routeDataMax.Exists(routeKey) Then GoTo NextCol

        Dim startRow As Long, endRow As Long, startCol As Long, endCol As Long
        startRow = CLng(routeDataMin(routeKey))
        endRow = CLng(routeDataMax(routeKey))
        startCol = CLng(Val(wsCol.Cells(r, 5).Value2))
        endCol = CLng(Val(wsCol.Cells(r, 6).Value2))
        If startRow <= 0 Or endRow < startRow Or startCol <= 0 Or endCol < startCol Then GoTo NextCol

        wsMain.Range(wsMain.Cells(startRow, startCol), wsMain.Cells(endRow, endCol)).ClearContents
NextCol:
    Next r
End Sub

Private Function GetLegendAirlines(ByVal wsMain As Worksheet) As Collection
    Dim out As New Collection
    Dim c As Long
    For c = 2 To 250
        Dim v As String
        v = UCase(Trim(CStr(wsMain.Cells(2, c).Value2)))
        If Len(v) = 0 Then Exit For
        AddUnique out, v
    Next c
    Set GetLegendAirlines = out
End Function

Private Function GetLegendSignals(ByVal wsMain As Worksheet) As Collection
    Dim out As New Collection
    Dim c As Long
    For c = 2 To 250
        Dim t As String
        t = NormalizeSignalToken(CStr(wsMain.Cells(3, c).Value2))
        If Len(t) = 0 Then Exit For
        AddUnique out, t
    Next c
    If out.Count = 0 Then
        AddUnique out, "INCREASE"
        AddUnique out, "DECREASE"
        AddUnique out, "NEW"
        AddUnique out, "SOLD OUT"
        AddUnique out, "UNKNOWN"
    End If
    Set GetLegendSignals = out
End Function

Private Function StateCell(ByVal kind As String) As String
    If LCase(kind) = "air" Then
        StateCell = "B2"
    Else
        StateCell = "B3"
    End If
End Function

Private Function GetUniverse(ByVal kind As String, ByVal wsMain As Worksheet) As Collection
    If LCase(kind) = "air" Then
        Set GetUniverse = GetLegendAirlines(wsMain)
    Else
        Set GetUniverse = GetLegendSignals(wsMain)
    End If
End Function

Private Function GetSelected(ByVal kind As String, ByVal wsCtl As Worksheet, ByVal wsMain As Worksheet) As Collection
    Dim allVals As Collection
    Set allVals = GetUniverse(kind, wsMain)

    Dim raw As String
    raw = CStr(wsCtl.Range(StateCell(kind)).Value2)
    Dim parsed As Collection
    Set parsed = CsvToCollection(raw)

    If parsed.Count = 0 Then
        Set GetSelected = allVals
        Exit Function
    End If

    Dim out As New Collection
    Dim item As Variant
    For Each item In parsed
        If CollectionContains(allVals, UCase(CStr(item))) Then
            AddUnique out, UCase(CStr(item))
        End If
    Next item

    If out.Count = 0 Then
        Set out = allVals
    End If
    Set GetSelected = out
End Function

Private Sub SetSelected(ByVal kind As String, ByVal wsCtl As Worksheet, ByVal coll As Collection)
    wsCtl.Range(StateCell(kind)).Value2 = CollectionToCsv(coll)
End Sub

Private Sub ToggleSelection(ByVal kind As String, ByVal token As String)
    Dim wsCtl As Worksheet, wsMain As Worksheet
    Set wsCtl = ThisWorkbook.Worksheets("Macro Control")
    Set wsMain = ThisWorkbook.Worksheets("Route Flight Fare Monitor")

    token = UCase(Trim(token))
    If Len(token) = 0 Then Exit Sub

    Dim sel As Collection, allVals As Collection
    Set sel = GetSelected(kind, wsCtl, wsMain)
    Set allVals = GetUniverse(kind, wsMain)

    Dim nextVals As Collection
    Set nextVals = New Collection

    If CollectionEquals(sel, allVals) Then
        AddUnique nextVals, token
    ElseIf sel.Count = 1 And CollectionContains(sel, token) Then
        Set nextVals = CloneCollection(allVals)
    Else
        Set nextVals = CloneCollection(sel)
        If CollectionContains(nextVals, token) Then
            On Error Resume Next
            nextVals.Remove token
            On Error GoTo 0
        Else
            AddUnique nextVals, token
        End If
        If nextVals.Count = 0 Then
            Set nextVals = CloneCollection(allVals)
        End If
    End If

    SetSelected kind, wsCtl, nextVals
End Sub

Private Sub RefreshLegendSelectionStyling()
    Dim wsCtl As Worksheet, wsMain As Worksheet
    Set wsCtl = ThisWorkbook.Worksheets("Macro Control")
    Set wsMain = ThisWorkbook.Worksheets("Route Flight Fare Monitor")

    Dim selAir As Collection, selSig As Collection
    Set selAir = GetSelected("air", wsCtl, wsMain)
    Set selSig = GetSelected("sig", wsCtl, wsMain)

    Dim c As Long
    For c = 2 To 250
        Dim at As String
        at = UCase(Trim(CStr(wsMain.Cells(2, c).Value2)))
        If Len(at) = 0 Then Exit For
        wsMain.Cells(2, c).Font.Strikethrough = Not CollectionContains(selAir, at)
        wsMain.Cells(2, c).Font.Bold = CollectionContains(selAir, at)
    Next c

    For c = 2 To 250
        Dim st As String
        st = NormalizeSignalToken(CStr(wsMain.Cells(3, c).Value2))
        If Len(st) = 0 Then Exit For
        wsMain.Cells(3, c).Font.Strikethrough = Not CollectionContains(selSig, st)
        wsMain.Cells(3, c).Font.Bold = CollectionContains(selSig, st)
    Next c

    On Error Resume Next
    wsMain.Shapes("btnMainModeContext").Fill.ForeColor.RGB = IIf(IsModeStrict(wsCtl), RGB(216, 228, 242), RGB(79, 129, 189))
    wsMain.Shapes("btnMainModeContext").TextFrame.Characters.Font.Color = IIf(IsModeStrict(wsCtl), RGB(31, 78, 120), RGB(255, 255, 255))
    wsMain.Shapes("btnMainModeStrict").Fill.ForeColor.RGB = IIf(IsModeStrict(wsCtl), RGB(192, 80, 77), RGB(242, 220, 219))
    wsMain.Shapes("btnMainModeStrict").TextFrame.Characters.Font.Color = IIf(IsModeStrict(wsCtl), RGB(255, 255, 255), RGB(128, 0, 0))
    On Error GoTo 0
End Sub

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

    Dim prevEvents As Boolean, prevScreen As Boolean
    prevEvents = Application.EnableEvents
    prevScreen = Application.ScreenUpdating
    Application.EnableEvents = False
    Application.ScreenUpdating = False

    Dim wsCtl As Worksheet, wsMain As Worksheet, wsBlock As Worksheet, wsRow As Worksheet
    Set wsCtl = ThisWorkbook.Worksheets("Macro Control")
    Set wsMain = ThisWorkbook.Worksheets("Route Flight Fare Monitor")
    Set wsBlock = ThisWorkbook.Worksheets("Route Block Index")
    Set wsRow = ThisWorkbook.Worksheets("Route Row Index")

    RestoreMainSheetFromBase

    Dim selAir As Collection, selSig As Collection
    Dim allAir As Collection, allSig As Collection
    Set selAir = GetSelected("air", wsCtl, wsMain)
    Set selSig = GetSelected("sig", wsCtl, wsMain)
    Set allAir = GetLegendAirlines(wsMain)
    Set allSig = GetLegendSignals(wsMain)

    Dim airFilterActive As Boolean, sigFilterActive As Boolean, strictMode As Boolean
    airFilterActive = Not CollectionEquals(selAir, allAir)
    sigFilterActive = Not CollectionEquals(selSig, allSig)
    strictMode = IsModeStrict(wsCtl)

    wsMain.Rows.Hidden = False
    wsMain.Rows("1:4").Hidden = False

    Dim blocks As Object, routeHasMatch As Object, routeRowKeep As Object
    Dim routeDataMin As Object, routeDataMax As Object
    Set blocks = CreateObject("Scripting.Dictionary")
    Set routeHasMatch = CreateObject("Scripting.Dictionary")
    Set routeRowKeep = CreateObject("Scripting.Dictionary")
    Set routeDataMin = CreateObject("Scripting.Dictionary")
    Set routeDataMax = CreateObject("Scripting.Dictionary")

    Dim lastBlock As Long, r As Long
    lastBlock = wsBlock.Cells(wsBlock.Rows.Count, 1).End(xlUp).Row
    For r = 2 To lastBlock
        Dim routeKey As String
        routeKey = UCase(Trim(CStr(wsBlock.Cells(r, 1).Value2)))
        If Len(routeKey) = 0 Then GoTo NextBlock

        Dim bStart As Long, bEnd As Long
        bStart = CLng(Val(wsBlock.Cells(r, 2).Value2))
        bEnd = CLng(Val(wsBlock.Cells(r, 3).Value2))
        If bStart <= 0 Or bEnd < bStart Then GoTo NextBlock

        Dim bAirCsv As String
        bAirCsv = CStr(wsBlock.Cells(r, 4).Value2)

        Dim keepRoute As Boolean
        keepRoute = True
        If airFilterActive Then keepRoute = CsvIntersectsSelection(bAirCsv, selAir)

        If keepRoute Then
            blocks(routeKey) = Array(bStart, bEnd)
            routeHasMatch(routeKey) = False
            Set routeRowKeep(routeKey) = CreateObject("Scripting.Dictionary")
        Else
            wsMain.Rows(CStr(bStart) & ":" & CStr(bEnd)).Hidden = True
        End If
NextBlock:
    Next r

    Dim lastRowIdx As Long
    lastRowIdx = wsRow.Cells(wsRow.Rows.Count, 1).End(xlUp).Row
    For r = 2 To lastRowIdx
        Dim rrRoute As String
        rrRoute = UCase(Trim(CStr(wsRow.Cells(r, 1).Value2)))
        If Len(rrRoute) = 0 Then GoTo NextRouteRow
        If Not blocks.Exists(rrRoute) Then GoTo NextRouteRow

        Dim rowNum As Long
        rowNum = CLng(Val(wsRow.Cells(r, 2).Value2))
        If rowNum <= 0 Then GoTo NextRouteRow

        Dim rowAirCsv As String, rowSigCsv As String, rowAirSigCsv As String
        rowAirCsv = CStr(wsRow.Cells(r, 4).Value2)
        rowSigCsv = CStr(wsRow.Cells(r, 5).Value2)
        rowAirSigCsv = CStr(wsRow.Cells(r, 6).Value2)

        Dim keepRow As Boolean
        If strictMode Then
            keepRow = RowMatchesStrict(rowAirCsv, rowSigCsv, rowAirSigCsv, selAir, selSig, airFilterActive, sigFilterActive)
        Else
            keepRow = RowMatchesContext(rowAirCsv, rowSigCsv, selAir, selSig, airFilterActive, sigFilterActive)
        End If

        If keepRow Then
            Dim keepMap As Object
            Set keepMap = routeRowKeep(rrRoute)
            keepMap(CStr(rowNum)) = True
            routeHasMatch(rrRoute) = True
        End If

        If Not routeDataMin.Exists(rrRoute) Then
            routeDataMin(rrRoute) = rowNum
            routeDataMax(rrRoute) = rowNum
        Else
            If rowNum < CLng(routeDataMin(rrRoute)) Then routeDataMin(rrRoute) = rowNum
            If rowNum > CLng(routeDataMax(rrRoute)) Then routeDataMax(rrRoute) = rowNum
        End If
NextRouteRow:
    Next r

    Dim routeKeyIter As Variant
    For Each routeKeyIter In blocks.Keys
        Dim blk As Variant
        blk = blocks(routeKeyIter)
        Dim startRow As Long, endRow As Long
        startRow = CLng(blk(0))
        endRow = CLng(blk(1))

        If Not CBool(routeHasMatch(routeKeyIter)) Then
            wsMain.Rows(CStr(startRow) & ":" & CStr(endRow)).Hidden = True
            GoTo NextRouteFinalize
        End If

        Dim dataMin As Long, dataMax As Long
        If routeDataMin.Exists(routeKeyIter) Then
            dataMin = CLng(routeDataMin(routeKeyIter))
            dataMax = CLng(routeDataMax(routeKeyIter))
        Else
            dataMin = startRow
            dataMax = endRow
        End If
        If dataMin < startRow Then dataMin = startRow
        If dataMax > endRow Then dataMax = endRow

        If dataMin > startRow Then
            wsMain.Rows(CStr(startRow) & ":" & CStr(dataMin - 1)).Hidden = False
        End If
        If dataMax >= dataMin Then
            wsMain.Rows(CStr(dataMin) & ":" & CStr(dataMax)).Hidden = True
        End If

        Dim k As Variant
        Dim routeKeepRows As Object
        Set routeKeepRows = routeRowKeep(routeKeyIter)
        For Each k In routeKeepRows.Keys
            wsMain.Rows(CLng(k)).Hidden = False
        Next k

        If endRow > dataMax Then
            wsMain.Rows(CStr(dataMax + 1) & ":" & CStr(endRow)).Hidden = False
        End If
NextRouteFinalize:
    Next routeKeyIter

    If strictMode Then
        ApplyStrictColumnMask selAir, airFilterActive, routeHasMatch, routeDataMin, routeDataMax
    End If

    RefreshLegendSelectionStyling
    wsMain.Activate
Done:
    Application.EnableEvents = prevEvents
    Application.ScreenUpdating = prevScreen
    Exit Sub
EH:
    MsgBox "ApplyMainSheetFilters failed: " & Err.Description, vbExclamation
    Resume Done
End Sub

Public Sub ClearMainSheetFilters()
    Dim wsCtl As Worksheet
    Set wsCtl = ThisWorkbook.Worksheets("Macro Control")
    wsCtl.Range("B2").Value2 = ""
    wsCtl.Range("B3").Value2 = ""
    ApplyMainSheetFilters
End Sub

Public Sub SetMainModeContext()
    ThisWorkbook.Worksheets("Macro Control").Range("B4").Value2 = MODE_CONTEXT
    ApplyMainSheetFilters
End Sub

Public Sub SetMainModeStrict()
    ThisWorkbook.Worksheets("Macro Control").Range("B4").Value2 = MODE_STRICT
    ApplyMainSheetFilters
End Sub

Public Sub HandleLegendClick(ByVal ws As Worksheet, ByVal Target As Range)
    If ws Is Nothing Or Target Is Nothing Then Exit Sub
    If ws.Name <> "Route Flight Fare Monitor" Then Exit Sub
    If Target.CountLarge <> 1 Then Exit Sub

    Dim r As Long, c As Long
    r = Target.Row
    c = Target.Column

    If r = 2 Then
        If c = 1 Then
            ThisWorkbook.Worksheets("Macro Control").Range("B2").Value2 = ""
            ApplyMainSheetFilters
            Exit Sub
        End If
        Dim airToken As String
        airToken = UCase(Trim(CStr(Target.Value2)))
        If Len(airToken) = 0 Then Exit Sub
        If CollectionContains(GetLegendAirlines(ws), airToken) Then
            ToggleSelection "air", airToken
            ApplyMainSheetFilters
        End If
        Exit Sub
    End If

    If r = 3 Then
        If c = 1 Then
            ThisWorkbook.Worksheets("Macro Control").Range("B3").Value2 = ""
            ApplyMainSheetFilters
            Exit Sub
        End If
        Dim sigToken As String
        sigToken = NormalizeSignalToken(CStr(Target.Value2))
        If Len(sigToken) = 0 Then Exit Sub
        If CollectionContains(GetLegendSignals(ws), sigToken) Then
            ToggleSelection "sig", sigToken
            ApplyMainSheetFilters
        End If
        Exit Sub
    End If
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

        $wsComp = $vbProj.VBComponents.Item($wsMain.CodeName)
        $wsCode = @"
Option Explicit

Private Sub Worksheet_SelectionChange(ByVal Target As Range)
    On Error Resume Next
    RouteMonitorFilters.HandleLegendClick Me, Target
End Sub
"@
        $lineCount = $wsComp.CodeModule.CountOfLines
        if ($lineCount -gt 0) {
            $wsComp.CodeModule.DeleteLines(1, $lineCount)
        }
        $wsComp.CodeModule.AddFromString($wsCode) | Out-Null
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

    # In-sheet click-action controls on current monitor tab.
    $shapeNamesMain = @()
    foreach ($shape in @($wsMain.Shapes)) {
        $n = [string]$shape.Name
        if (
            $n -eq "btnClearMainFilters" -or
            $n -eq "btnMainModeContext" -or
            $n -eq "btnMainModeStrict" -or
            $n -like "mflt_air_*" -or
            $n -like "mflt_sig_*"
        ) {
            $shapeNamesMain += $n
        }
    }
    foreach ($n in $shapeNamesMain) {
        try { $wsMain.Shapes.Item($n).Delete() } catch {}
    }

    foreach ($cb in @($wsMain.CheckBoxes())) {
        $n = [string]$cb.Name
        if ($n -like "mflt_air_*" -or $n -like "mflt_sig_*") {
            try { $cb.Delete() } catch {}
        }
    }

    $legendLastCol = [int][Math]::Max(
        [int]$wsMain.Cells.Item(2, $wsMain.Columns.Count).End(-4159).Column,
        [int]$wsMain.Cells.Item(3, $wsMain.Columns.Count).End(-4159).Column
    )
    if ($legendLastCol -lt 1) { $legendLastCol = 1 }
    $legendRange = $wsMain.Range($wsMain.Cells.Item(2, 1), $wsMain.Cells.Item(3, $legendLastCol))
    $legendRange.Borders.LineStyle = 1
    $legendRange.Borders.Weight = 2
    $legendRange.HorizontalAlignment = -4108 # xlCenter
    $legendRange.VerticalAlignment = -4108   # xlCenter
    $wsMain.Range("A2:A3").Interior.Color = 15132390
    $wsMain.Range("A2:A3").Font.Bold = $true

    $anchorCol = 28
    $baseLeft = [double]$wsMain.Cells.Item(1, $anchorCol).Left
    $baseTop = [double]$wsMain.Cells.Item(1, $anchorCol).Top
    $wsMain.Cells.Item(1, $anchorCol).Value2 = "Interactive Actions (Click Legends)"
    $wsMain.Cells.Item(2, $anchorCol).Value2 = "Click airline/signal legend cells to toggle selections."
    $wsMain.Cells.Item(3, $anchorCol).Value2 = "Context keeps full route view; Strict shows selected airlines only."

    $btnModeContext = $wsMain.Shapes.AddShape(1, $baseLeft, $baseTop + 62, 95, 24)
    $btnModeContext.Name = "btnMainModeContext"
    $btnModeContext.TextFrame.Characters().Text = "Mode: Context"
    $btnModeContext.OnAction = "SetMainModeContext"

    $btnModeStrict = $wsMain.Shapes.AddShape(1, $baseLeft + 102, $baseTop + 62, 90, 24)
    $btnModeStrict.Name = "btnMainModeStrict"
    $btnModeStrict.TextFrame.Characters().Text = "Mode: Strict"
    $btnModeStrict.OnAction = "SetMainModeStrict"

    $btnMainB = $wsMain.Shapes.AddShape(1, $baseLeft, $baseTop + 90, 200, 24)
    $btnMainB.Name = "btnClearMainFilters"
    $btnMainB.TextFrame.Characters().Text = "Clear Main Sheet Filters"
    $btnMainB.OnAction = "ClearMainSheetFilters"

    try { $excel.Run("ApplyMainSheetFilters") | Out-Null } catch {}

    $wb.Save()
    Write-Output "xlsm_exported=$outPath"
} finally {
    if ($wb -ne $null) { $wb.Close($true) | Out-Null }
    if ($excel -ne $null) {
        $excel.Quit() | Out-Null
        [System.Runtime.InteropServices.Marshal]::ReleaseComObject($excel) | Out-Null
    }
}
