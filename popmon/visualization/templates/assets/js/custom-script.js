// hide all except first feature data for each section
$("section").each(function() {
    $(this).find(".section_feature:not(:first)").hide();
});

// show corresponding feature's data based on the filter
$(document).on("click", "button.dropdown-item", function() {
    obj = $(this)

    // Linked dropdowns
    $("div.section_feature").hide()
    $("div[data-section-feature='" + obj.attr("data-feature") + "']").show()
    $("button.dropdown-toggle").text("Feature: " + obj.text())

    // Current section (if any), e.g. #histograms
    var type = window.location.hash.substr(1);
    if (type.length > 0){
        // Find link to that section
        var o = $("a.nav-link.js-scroll-trigger[href='#" + type +"'"    );

        // If exists
        if (o.length == 1){
            // Move to that location
            var offset = $("section[data-section-title='" + o.attr("data-scroll-to-section") + "']").offset().top;
            window.scrollTo(0, offset);
        }
    }
});

$(document).on("click", "a.table-item", function(){
    obj = $(this)
    $("button.dropdown-item[data-feature='" + obj.attr("data-feature") + "']").click()
});

// making navigation work: after clicking a nav link scrolling to the corresponding section's position
$(document).on("click", "a.nav-link,a.navbar-brand", function(e) {
    obj = $(this)
    $([document.documentElement, document.body]).animate({
        scrollTop: $("section[data-section-title='" + obj.attr("data-scroll-to-section") + "']").offset().top
    }, 1000);
});

function slugify(str) {
    // Convert string to id used in url
    str = str.replace(/^\s+|\s+$/g, '').toLowerCase();
    str = str.replace(/[^a-z0-9 -]/g, '').replace(/\s+/g, '-').replace(/-+/g, '-');
    return str;
};

// automatic insertion of navigation links based on section titles
$('section').each(function(i, el){
    title = $(this).attr("data-section-title");
    slug = slugify(title)
    $(this).attr('id', slug);
    code = '<li class="nav-item"><a class="nav-link js-scroll-trigger" data-scroll-to-section="' + title + '" href="#'+ slug +'">' + title + '</a></li>'
    $("ul#navigation-sections").append(code);
    if ( i === 0 ) {
        $("a.navbar-brand").attr('data-scroll-to-section', title);
    }
});

$('#myModal').on('shown.bs.modal', function () {
    $('#myInput').trigger('focus')
});

$("#toggleDescriptions").change(function() {
    if(this.checked) {
        $("p.card-text").show();
    } else {
        $("p.card-text").hide();
    }
});

var plotly_config = {scrollZoom: true, displaylogo: false, modeBarButtonsToRemove: ['lasso2d']};


const deepCopy = (inObject) => {
      let outObject, value, key
      if (typeof inObject !== "object" || inObject === null) {
        return inObject
      }

      outObject = Array.isArray(inObject) ? [] : {}

      for (key in inObject) {
        value = inObject[key]
        outObject[key] = deepCopy(value)
      }

      return outObject
}