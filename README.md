# scraping-example
requests-html と asyncio を使った Web Scraping スクリプトの習作。

抽象クラス Downloader をつくってみた。
prepare, download, save メソッドによりどのように最初のダウンロード対象を決めるか、
どうダウンロードしてどのように追加のダウンロード対象を決めるか、どこに保存するかを決められる。

そして HTML 文章を指す URL を渡すと img タグを探してただダウンロードするだけの ImgDownloader をつくってみた。
