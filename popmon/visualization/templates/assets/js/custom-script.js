// hide all except first feature data for each section
$( "section" ).each(function() {
    $( this ).find(".section_feature:not(:first)").hide();
});
// show corresponding feature's data based on the filter
$(document).on("click", "button.dropdown-item", function() {
    obj = $(this)

//    obj.closest("section").find("div.section_feature").hide()
//    obj.closest("section").find("div[data-section-feature='" + obj.attr("data-feature") + "']").show()
//    obj.parent().siblings("button").text("Feature: " + obj.text())

    // Linked dropdowns
    $("div.section_feature").hide()
    $("div[data-section-feature='" + obj.attr("data-feature") + "']").show()
    $("button.dropdown-toggle").text("Feature: " + obj.text())
});
// making navigation work: after clicking a nav link scrolling to the corresponding section's position
$(document).on("click", "a.nav-link,a.navbar-brand", function(e) {
    e.preventDefault();
    obj = $(this)
    $([document.documentElement, document.body]).animate({
        scrollTop: $("section[data-section-title='" + obj.attr("data-scroll-to-section") + "']").offset().top
    }, 1000);
});
// automatic insertion of navigation links based on section titles
$('section').each(function(i, el){
    title = $(this).attr("data-section-title");
    code = '<li class="nav-item"><a class="nav-link js-scroll-trigger" data-scroll-to-section="' + title + '">' + title + '</a></li>'
    $("ul#navigation-sections").append(code);
    if ( i === 0) {
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
