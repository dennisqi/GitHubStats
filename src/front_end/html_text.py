html_text = '''
<!DOCTYPE html>
<html>
    <head>
        <title>Repo Census</title>
        <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.2.1/css/bootstrap.min.css" integrity="sha384-GJzZqFGwb1QTTN6wy59ffF1BuGJpLSa9DkKMp0DgiMDm4iYMj70gZWKYbI706tWS" crossorigin="anonymous">
        {%css%}
    </head>
    <body>
        <header>
            <div class="collapse bg-dark" id="navbarHeader">
                <div class="container">
                    <div class="row">
                        <div class="col-sm-8 col-md-7 py-4">
                            <h4 class="text-white">About</h4>
                            <p class="text-muted">Add some information about the album below, the author, or any other background context. Make it a few sentences long so folks can pick up some informative tidbits. Then, link them off to some social
                                networking sites or contact information.</p>
                        </div>
                        <div class="col-sm-4 offset-md-1 py-4">
                            <h4 class="text-white">Contact</h4>
                            <ul class="list-unstyled">
                                <li><a href="#" class="text-white">Follow on Twitter</a></li>
                                <li><a href="#" class="text-white">Like on Facebook</a></li>
                                <li><a href="#" class="text-white">Email me</a></li>
                            </ul>
                        </div>
                    </div>
                </div>
            </div>
            <div class="navbar navbar-dark bg-dark shadow-sm">
                <div class="container d-flex justify-content-between">
                    <a href="#" class="navbar-brand d-flex align-items-center" style="text-decoration: none">
                        <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/1/1d/Line_chart_icon_Noun_70892_cc_White.svg/2000px-Line_chart_icon_Noun_70892_cc_White.svg.png" alt="" width="20" height="20" class="mr-1">
                        <strong>GGM</strong>
                    </a>
                </div>
            </div>
        </header>
        <div class="container">
            <section class="jumbotron text-center mt-5">
                <div class="container">
                    <p>
                        GitHub is one of the largest open source community in the world. Repo Census aims to measure the history and creation of GitHub repositories
                    </p>
                </div>
            </section>
            {%app_entry%}
            <footer class="text-muted" style="padding-top:3rem; padding-bottom:3rem">
                {%config%}
                {%scripts%}
                <div class="container">
                    <p class="float-right">
                        <a href="#">Back to top</a>
                    </p>
                    Dataset from <a href="www.gharchive.org">gharchive</a>
                </div>
            </footer>
        </div>
    </body>
</html>
'''