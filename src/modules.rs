pub fn url_encode(input: &str) -> String {
       input
       .replace("%", "%25")  // Must be first
       .replace("#", "%23")
       .replace("&", "%26")
       .replace("@", "%40")
       .replace("?", "%3F")
       .replace("=", "%3D")
       .replace("+", "%2B")
       .replace(" ", "%20")
       .replace(":", "%3A")
       .replace("/", "%2F")
}
