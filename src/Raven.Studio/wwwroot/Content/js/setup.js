// JavaScript Document

(function (window) {
    $('.btn-toggle').click(function (e) {
        var target = $(this).attr('data-target');
        var targetClass = $(this).attr('data-class');
        $(target).toggleClass(targetClass);
    });

    var $mainMenu = $('#main-menu');

    $('#main-menu a').click(function(e) {		
        var $list = $(this).closest('ul');
        var hasOpenSubmenus = $list.find('.level-show').length;
        var isOpenable = $(this).siblings('.level').length;
        
        if (!hasOpenSubmenus && isOpenable) {
            $(this).parent().children('.level').addClass('level-show');
            e.stopPropagation();	
        }
        
        setMenuLevelClass();
    });
    
    $('#main-menu ul').click(function(e) {	
        $(this).find('.level-show').removeClass('level-show');
        e.stopPropagation();	
        setMenuLevelClass();	
    });
    
    $('#main-menu .back').click(function() {
                
    });
    
    $('.menu-collapse-button').click(function() {
        $('body').toggleClass('menu-collapse');
        
    });

/* window.addEventListener('orientationchange',adjustSize, false);

window.onresize = function(event) {
    adjustSize();
}

$(document).ready(function(e) {
    adjustSize();
});

function adjustSize(){
    if ($(window).width() > 768) {
        var height = $('.owl-stage').height()+20;
        $('.owl-height').css('height',height);		
    }
    else {
        //$('#map').css('width','100%');		
    }

    
}
*/
function setMenuLevelClass() {
    var level = $mainMenu.find('.level-show');
    $mainMenu.attr('data-level', level.length);
}

}( window ));
