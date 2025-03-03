#r "nuget: System.Text.Encoding.CodePages"

open System
open System.Net
open System.Text
open System.IO
open System.Text.RegularExpressions
open System.Net.Http

// Register encoding provider to support Windows-1251
Encoding.RegisterProvider(CodePagesEncodingProvider.Instance)

// Create a single HttpClient instance to be reused
let httpClient = new HttpClient()

//just a short snippet to measure time spent in an eagerly executed function
//not gonna work with lazy function, e.g. function returning a sequence (IEnumerable)
let time jobName job = 
    let startTime = DateTime.Now;
    let returnValue = job()
    let endTime = DateTime.Now;
    printfn "%s took %d ms" jobName (int((endTime - startTime).TotalMilliseconds))
    returnValue

//goes through 2 lists in linear time and looks for equal keys
//for elements with equal keys a given function is called to produce output merged element
//NOTE:
//1. function assumes that keys in the second list are unique, otherwise results will be surprising, see (*) below
//2. function assumes both lists are ordered ascending
let rec orderedListsMerge xs ys keyExtractor merger =
    match xs, ys with
    | [],_ | _,[] -> []
    | x::xs', y::ys' ->
        let xkey = keyExtractor x
        let ykey = keyExtractor y
        if(xkey = ykey) then
            //here we move xs forward, but keep ys the same,
            //because we assume that next y will have different key while next x might still have the same key, 
            //otherwise this logic is incorrect
            (merger x y) :: orderedListsMerge xs' ys keyExtractor merger            // (*)
        elif(xkey > ykey) then
            orderedListsMerge xs ys' keyExtractor merger
        else
            orderedListsMerge xs' ys keyExtractor merger

let webRequestHtml (url : string) =
    // Use async computation expression
    let asyncResult = async {
        let! response = httpClient.GetAsync(url) |> Async.AwaitTask
        let! content = response.Content.ReadAsByteArrayAsync() |> Async.AwaitTask
        return Encoding.GetEncoding("Windows-1251").GetString(content)
    }
    // Execute async code synchronously (since the rest of the code is sync)
    Async.RunSynchronously asyncResult

let regexSingleLineMatch input pattern =
    Regex.Match(input, pattern, RegexOptions.Singleline).Groups.Item(1).Value

let regexMatches input pattern =
    seq { for m in Regex.Matches(input, pattern) -> m.Groups.Item(1).Value }

let regexSingleLineMatches input pattern =
    seq { for m in Regex.Matches(input, pattern, RegexOptions.Singleline) -> m.Groups.Item(1).Value }

//only named hrefs point to poems
let extractNamedHrefs html = 
    //I tried XmlDocument here, but it doesn't work as HTML can contain some "invalid" elements like &nbsp;
    //Stand back now, I'm going to use regular expressions!
    let hrefPattern = "<a name=.* href=\"(.+?)\">.+?</a>"
    regexMatches html hrefPattern


//remove all html markup from the line
let cleanupHtml text =
    let htmlTagPattern = new Regex("<[^>]*>", RegexOptions.Compiled)
    let withoutTags = htmlTagPattern.Replace(text, String.Empty)
    let newlinePattern = new Regex("[\r\n]+", RegexOptions.Compiled)
    newlinePattern.Replace(withoutTags, " ")

    
let takeFirstLine text =
    let firstLinePattern = "(.*)"
    Regex.Match(text, firstLinePattern).Groups.Item(1).Value


type Poem(poemHref : string, title : string, lines : seq<string>) =
    let MAX_TITLE_LENGTH = 30
    member this.Href = poemHref
    member this.Title =
        let newTitle = 
            match title with
            | "* * *" -> (lines |> Seq.item 0)
            | _ -> title
        if(newTitle.Length > MAX_TITLE_LENGTH) then
            newTitle.Substring(0, MAX_TITLE_LENGTH-3) + "..."
        else
            newTitle

    member this.Lines = 
        seq {
            for line in lines ->
                let nbspPattern = "&nbsp;"
                Regex.Replace(line, nbspPattern, "")
            }
    member this.LineTokens =
        seq { 
            for line in lines ->
                let russianWordPattern = "([а-яА-Я]+)"
                regexMatches (line.ToLower()) russianWordPattern
            }

//TODO: add more structural analysis -> handle sub-titles and personas
let producePoem poemHref poemHtml =
    //titles can be multiline, sometimes they include sub-titles
    let titlePattern = "<h1>(.+?)</h1>" 
    let linePattern = "<span class=\"line[^>]*>(.+?)</span>"
    new Poem(
        poemHref,
        (regexSingleLineMatch poemHtml titlePattern) |> cleanupHtml |> takeFirstLine, 
        regexSingleLineMatches poemHtml linePattern |> Seq.map cleanupHtml)


//check that the given link is a link to a final edition poem, not early edition to avoid duplicate texts in index
let isFinalEditionHref (href : string) =
    not (href.Contains("03edit"))

let crawlPoems =
    let domainUrl = "http://www.rvb.ru/pushkin/"
    let volumeUrlTemplate = domainUrl + "tocvol{0}.htm"
    let poemUrlTemplate = domainUrl + "{0}"

    //take only first 4 volumes -- they contain poems
    seq { for volumeNumber in 1..4 -> String.Format(volumeUrlTemplate, volumeNumber) }
        |> Seq.map webRequestHtml
        |> Seq.collect extractNamedHrefs
        |> Seq.filter isFinalEditionHref 
        |> Seq.map (fun href -> String.Format(poemUrlTemplate, href))

//        //development mode -- comment later
//        |> Seq.take 40

        //requesting individual poems
        |> Seq.map (fun href -> (producePoem href (webRequestHtml href)))
        |> Seq.cache
        

//building inversed index of tokens in poems
//so that we have a way to index (token -> poem number -> (line number,position in line))
let indexPoems (poems : seq<Poem>) = 
    poems
        |> Seq.mapi 
        (
            fun poemNumber poem -> 
                poem.LineTokens 
                    |> Seq.mapi 
                    (
                        fun lineNumber tokens ->
                            tokens
                                |> Seq.mapi 
                                (
                                    fun position token ->
                                        (token, poemNumber, lineNumber, position)
                                )
                    )
                    |> Seq.concat
        )
        |> Seq.concat

        //now we have raw list of tuples, we will turn it into ordered inversed index

        |> Seq.groupBy (fun (token, _, _, _) -> token)
        |> Seq.sortBy (fun (token, _) -> token)
        |> Seq.map 
        (
            fun (token, tuples) ->
                let poems =
                    tuples 
                        |> Seq.map ( fun (token, poemNumber, lineNumber, position) -> (poemNumber,lineNumber,position) )
                        |> Seq.groupBy (fun (poemNumber,lineNumber,position) -> poemNumber)
                        |> Seq.sortBy (fun (poemNumber, _) -> poemNumber)
                        |> Seq.map 
                        (
                            fun (poemNumber, tuples) ->
                                let linesPositions =
                                    tuples
                                        |> Seq.map (fun (poemNumber,lineNumber,position) -> (lineNumber,position))
                                        |> Seq.sortBy ( fun (lineNumber,position) -> position)
                                        |> Seq.sortBy ( fun (lineNumber,position) -> lineNumber)    //sortBy is stable according to MSDN
                                        |> Seq.toList
                                (poemNumber, linesPositions)
                        )
                        |> Seq.toList
                (token, poems)
        )
        |> Seq.toList

//token index is a subtree of full index only including poems and lines with the given token in given position
let tokenIndex fullIndex filterToken filterPosition =
    let (token, poems) = 
        fullIndex
            |> List.find (fun(token, poems) -> token=filterToken)
    poems
        |> List.map
        (
            fun (poemNumber, linesPositions) ->
                let filteredLines = linesPositions |> List.filter (fun (lineNumber, position) -> position = filterPosition)
                (poemNumber, filteredLines)
        )
        |> List.filter (fun (poemNumber, linesPositions) -> not (Seq.isEmpty linesPositions))


//intersect current index with token index
//we want to only keep tokens and poems which are present in the token index (which is a subtree of full index, see above)
let intersectIndex currentIndex tokenIndex =
    currentIndex
        |> List.map
        (
            fun (token, poems) ->
                let mergePoems currentPoems tokenPoems =
                    let mergeLinesPositions currentLinesPositions tokenLinesPositions =
                        let keyExtractor = (fun (lineNumber, _) -> lineNumber)
                        let merger = (fun (currentLineNumber, currentPosition) (_,_) -> (currentLineNumber, currentPosition))
                        orderedListsMerge currentLinesPositions tokenLinesPositions keyExtractor merger

                    let keyExtractor = (fun (poemNumber, _) -> poemNumber)
                    let merger = (fun (currentPoemNumber, currentLinesPositions) (_, tokenLinesPositions) -> (currentPoemNumber, mergeLinesPositions currentLinesPositions tokenLinesPositions))
                    orderedListsMerge currentPoems tokenPoems keyExtractor merger
                        |> List.filter (fun (poemNumber, linesPositions) -> not (List.isEmpty linesPositions))

                (token, mergePoems poems tokenIndex)
        )
        |> List.filter (fun (token, poems) -> not (List.isEmpty poems))


// The main function to query reverse index
// index -- index per se, we assume that the index is already filtered by caller using intersect\tokenFilter
// findPosition -- number of position to search tokens for
//                 the query function will return a list of terms that can be in this position
// count -- number of most frequent terms to return
let queryIndex index findPosition count =
    index 
        |> List.map
        (
            fun (token, poems) ->
                let tokenFreq = 
                    poems
                        |> List.sumBy
                        (
                            fun (_, linesPositions) ->
                                linesPositions
                                    |> List.sumBy
                                    (
                                        fun (lineNumber, position) ->
                                            if (position = findPosition) then 1 else 0
                                    )
                        )
                (token, tokenFreq)
        )
        |> Seq.filter (fun (token, tokenFreq) -> tokenFreq > 0)
        |> Seq.sortBy (fun (token, tokenFreq) -> -tokenFreq)
        |> Seq.zip [1..count] // Seq.take fails if there is less than "count" elements
        |> Seq.map (fun (index, element) -> element)
        |> Seq.toList

//acquire first poem for given token and position
//used to resolve single query result token into poems
let getPoemResult index findToken findPosition =
    let (token, poems) = 
        index
            |> List.find (fun (token, poems) -> token = findToken)
    
    poems
        |> List.collect
        (
            fun (poemNumber, linesPositions) ->
                linesPositions
                    |> List.filter (fun (lineNumber, position) -> position = findPosition)
                    |> List.map (fun (lineNumber, position) -> (poemNumber, lineNumber))
        )
        |> Seq.item 0


type QueryResult =
    //token + count
    | LineVariant       of string*int
    //poemNumber, lineNumber
    | SinglePoem    of int*int

//TODO: identical strings currently will not be resolved to their poems

//this is a wrapper around query index that will perform the same action,
//but the result will be translated and wrapped into QueryResult type
//single result tokens will be returned as (poemNumber, lineNumber) tuple
let wrappedQueryIndex filteredIndex searchPosition count =
    queryIndex filteredIndex searchPosition count
        |> List.map 
        (
            fun (token, count) ->
                match count with
                | 1 -> SinglePoem(getPoemResult filteredIndex token searchPosition)
                | _ -> LineVariant(token,count)
        )


type PrettyResult =
    | PrettyLineVariant       of string*int
    | PrettySinglePoem        of string*string*int*string

let prettifyQueryResults queryResults poems =
    queryResults
        |> List.map
        (
            fun result ->
                match result with
                | SinglePoem (poemNumber, lineNumber) -> 
                    let (poem : Poem) = 
                        poems
                            |> Seq.item poemNumber
                    let line =
                        poem.Lines
                            |> Seq.item lineNumber
                    PrettySinglePoem(poem.Title, poem.Href, lineNumber, line)
                | LineVariant (token, count) -> PrettyLineVariant(token,count)
        )

type PushkinTreeNode =
    | VariantNode     of string*int*seq<PushkinTreeNode>
    | PoemNode        of string*string*int*string

let createPushkinTree pushkinPoems poemsIndex count =
    let rec createTreeLevel count currentQuery currentIndex =
        let searchPosition = List.length currentQuery
        let queryResult = wrappedQueryIndex currentIndex searchPosition count
        let prettyResult = prettifyQueryResults queryResult pushkinPoems
        prettyResult
            |> List.map
            (
                fun result ->
                    match result with
                    | PrettySinglePoem (title, href, lineNumber, line) -> PoemNode(title, href, lineNumber, line)
                    | PrettyLineVariant (token, freq) -> VariantNode(token, freq, createTreeLevel count (currentQuery @ [token]) (intersectIndex currentIndex (tokenIndex currentIndex token searchPosition)))
            )
    createTreeLevel count [] poemsIndex

let resultsToHtml pushkinTree =
    let rec treeToHtml tree =
        let zippedTreeLevel = tree |> Seq.zip (Seq.initInfinite id)
        
        zippedTreeLevel
            |> Seq.fold
            (
                fun acc treeNode ->
                    match treeNode with
                    | (number, PoemNode (title, href, lineNumber, line)) ->
                        acc + sprintf "<div class='poem'><span class='line'>%s</span> <span class='line-source'>(<a target='blank' class='fromlink' href='%s'>%s</a>, строка %d)</span></div>\n" 
                            line href title (lineNumber+1)
                    | (number, VariantNode (token, freq, subtree)) ->
                        // Now each node is wrapped in its own container div that controls visibility
                        acc + sprintf "<div class='variant-container'><div class='variant'><span class='term' onclick='this.closest(\".variant-container\").classList.toggle(\"open\")'>%s &#8658; %d</span></div><div class='children'>%s</div></div>\n" 
                            token freq (treeToHtml subtree)
            ) ""
    
    treeToHtml pushkinTree

let outputCompleteHtml tree =
    let htmlPrelude = """<!DOCTYPE html>
<html>
<head>
    <title>Pushkin Tree</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .variant-container .children { display: none; margin-left: 20px; }
        .variant-container.open > .children { display: block; }
        .term { color: blue; cursor: pointer; }
        .term:hover { text-decoration: underline; }
        .line-source { font-style: italic; font-size: 0.5em; }
        .fromlink { text-decoration: none; }
        .fromlink:hover { text-decoration: underline; }
        .children { border-left: 1px solid #ddd; padding-left: 10px; }
    </style>
</head>
<body>"""

    let htmlEnd = """</body>
</html>"""
    
    let content = resultsToHtml tree
    File.WriteAllText("output.html", htmlPrelude + content + htmlEnd)

let poems = crawlPoems //lazy operation, so we can't time it here, we do it the in next line
printfn "Crawled %d poems" (time "Crawling poems" (fun() -> poems |> Seq.length))
printfn "Crawled %d lines" (poems |> Seq.sumBy ( fun poem -> poem.Lines |> Seq.length))
let poemIndex = time "Indexing poems" (fun () -> poems |> indexPoems)
printfn "Index contains %d terms" poemIndex.Length
let tree = time "Generating result tree" (fun () -> createPushkinTree poems poemIndex 20)
time "Output content" (fun() -> outputCompleteHtml tree)



