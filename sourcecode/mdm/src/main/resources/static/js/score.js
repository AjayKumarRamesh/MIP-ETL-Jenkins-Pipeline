/*!
 * Score.js - jQuery Star Rating Plugin
 */

(function($, window, undefined) {
    var methods = {
        init: function(option){
            return this.each(function(){
                var _this = this,
                    $this = $(this);
                option = this.option = $.extend({}, $.fn.score.defaults, $this.data(), option);

                this.raw = $(this).clone()[0];

                var itemType = this.tagName === 'UL' ? 'li' : 'span';
                var items = '';
                for (var i=0; i < option.number; i++)
                {
                    var hint = option.number - i;
                    if (option.hints){
                        hint = option.hints[hint-1] ? option.hints[hint-1] : hint;
                    }
                    items += '<'+ itemType + ' class="score-item" title="' + hint + '"></' +itemType + '>';
                }
                $this.addClass('scorejs').html(items).data('option',option)
                    .css({
                        'font-size': option.size+'px',
                        'color': option.color
                    });


                this.initStyle = {
                    fontSize: $this.css('font-size'),
                    color: $this.css('color')
                };


                debug.call(_this, $this, 'Initialization OptionData: ', option);

                if (option.readOnly){
                    methods.readOnly.call($this, true);
                }else{
                    methods._binds.call(_this);
                }

                if (option.fontAwesome){
                    $this.addClass('fontawesome');
                }

                if (option.vertical){
                    $this.addClass('score-vertical');
                }

                if (typeof Number(option.score) === 'number'){
                    methods.score.call($this, Number(option.score));
                }
            });
        },

        score: function(score){
            if (score){
                return this.each(function(){
                    var option = this.option;
                    score = score > option.number ? option.number : score;
                    var index = option.number - score;
                    $(this)
                        .children()
                        .removeClass('active')
                        .eq(index)
                        .addClass('active')
                        .nextAll()
                        .addClass('active')
                        .end()
                        .parent('.scorejs')
                        .data({
                            'index': index,
                            'score': score
                        });


                    debug.call(this, $(this), 'Score Set: ', score);
                });
            }else{
                return this.data('score');
            }
        },

        option: function(option){
            if (option){
                return this.each(function(){
                    var oriOption = this.option;
                    var newOption = $.extend({}, oriOption, option);

                    methods.destroy.call($(this));
                    methods.init.call($(this), newOption);


                    debug.call(this, $(this), 'Option Set: ', option, 'Original Option: ', oriOption, 'New Option: ', newOption);
                });
            }else{
                return this.data('option');
            }
        },

        readOnly: function(readOnly){
            return this.each(function(){
                if (readOnly){
                    $(this)
                        .addClass('read-only')
                        .off('.scorejs')
                        .children()
                        .removeClass('score-item')
                        .addClass('score-item-static')
                        .eq($(this).data('index'))
                        .addClass('active')
                        .nextAll()
                        .addClass('active');
                }else{
                    if ($(this).hasClass('read-only')){
                        $(this)
                            .removeClass('read-only')
                            .children()
                            .removeClass('score-item-static')
                            .addClass('score-item')

                        methods._binds.call(this);
                    }
                }


                debug.call(this, $(this), 'readOnly:', readOnly);
            });
        },

        cancel: function(){
            return this.each(function(){
                $(this).removeData('index score').children().removeClass('active');
                debug.call(this, $(this), 'Canceled');
            });
        },

        destroy: function(){
            return this.each(function(){
                $(this).off('.scorejs').empty().removeClass('scorejs read-only fontawesome score-vertical').removeData('index score option');
                if (this.style.fontSize === this.initStyle.fontSize){
                    this.style.fontSize = this.raw.style.fontSize;
                }
                if (this.style.color === this.initStyle.color){
                    this.style.color = this.raw.style.color;
                }
                if ($(this).attr('style') === ''){
                    $(this).removeAttr('style');
                }
                debug.call(this, $(this), 'Destroyed');

                delete this.option;
            });
        },

        _binds: function(){
            var _this = this,
                $this = $(this),
                option = this.option;
            $this.on({
                'click.scorejs': function(event){
                    var score = option.number - $(this).index();
                    methods.score.call($this, score);
                    setCallback.call(_this, event.type, score, event);
                },
                'mouseover.scorejs': function(event){
                    var score = option.number - $(this).index();
                    $this.children().removeClass('active');
                    setCallback.call(_this, event.type, score, event);
                },
                'mouseout.scorejs': function(event){
                    var score = methods.score.call($this);
                    $this.children()
                        .eq($(this).parent().data('index'))
                        .addClass('active')
                        .nextAll()
                        .addClass('active');
                    setCallback.call(_this, event.type, score, event);
                }
            }, '.score-item');
        }
    };

    function setCallback(callback){
        var callbackReference;
        if (typeof this.option[callback] === 'function'){
            callbackReference = this.option[callback];
            this.option[callback].apply(this, Array.prototype.slice.call(arguments, 1));
        }else{
            callbackReference = 'No callback function set';
        }
        debug.call(this, 'Callback Triggered: [',callback,'|',callbackReference,']');
    }

    function debug(){
        if (this.option.debug){
            var logger = window.console['debug'];
            if (typeof logger === 'function'){
                logger.apply(window.console, arguments);
            }
        }
    }

    $.fn.score = function (option) {
        if (methods[option]) {
            return methods[option].apply(this, Array.prototype.slice.call(arguments, 1));
        }else if (typeof option === "object" || !option) {
            return methods.init.apply(this, arguments);
        }
        return false;
    }

    $.fn.score.defaults = {
        number      : 5,
        size        : 40,
        color       : '#08C',
        score       : undefined,
        vertical    : false,
        hints       : undefined,
        click       : undefined,
        mouseover   : undefined,
        mouseout    : undefined,
        readOnly    : false,
        fontAwesome : false,
        custom      : false,
        debug       : false
    }
})(jQuery, window);