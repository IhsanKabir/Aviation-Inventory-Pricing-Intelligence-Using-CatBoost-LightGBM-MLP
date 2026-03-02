/**
 * @file
 * airastra behaviors.
 */

(function ($, Drupal) {
  ('use strict');
  /**
   * Behavior description.
   */
  Drupal.behaviors.airastra = {
    attach: function (context, settings) {
      const menuToggle = $('.mobile-menu-toggle', context);
      const mainMenu = $('.mobile-menu .nav-collapse', context);
      const dropdownToggle = $('.dropdown-toggle', context);

      dropdownToggle.on('click', function (e) {
        e.stopPropagation();
        const currentLink = $(this);
        currentLink.toggleClass('toggled');
      });

      menuToggle.on('click', (e) => {
        e.preventDefault();
        $('.toggle-icon', context).toggleClass('open');
        mainMenu.toggleClass('collapse');
        $('html').toggleClass('menu-open');
        mainMenu.css({ height: 'auto' });
      });
    },
  };

    /**
   * Adds slider to related holiday package section.
   */
    Drupal.behaviors.relatedHolidayPackageSlider = {
      attach: function (context, settings) {
        const relatedHolidayPackageSwiper = new Swiper('.related-holiday-packages', {
          slidesPerView: 'auto',
          spaceBetween: 16,
          navigation: {
            nextEl: '.related-holiday-packages-swiper-button-next',
            prevEl: '.related-holiday-packages-swiper-button-prev',
          },
        });
      },
    };

    /**
   * Adds slider to related holiday package section.
   */
      Drupal.behaviors.relatedOfferPackageSlider = {
        attach: function (context, settings) {
          const relatedOfferPackageSwiper = new Swiper('.related-offer-packages', {
            slidesPerView: 'auto',
            spaceBetween: 16,
            navigation: {
              nextEl: '.related-offer-packages-swiper-button-next',
              prevEl: '.related-offer-packages-swiper-button-prev',
            },
          });
        },
      };

  /**
   * Adds slider to offers-slider section.
   */
  Drupal.behaviors.offersSlider = {
    attach: function (context, settings) {
      const offersSwiper = new Swiper('.offers-slider', {
        slidesPerView: 'auto',
        spaceBetween: 16,
        navigation: {
          nextEl: '.offers-swiper-button-next',
          prevEl: '.offers-swiper-button-prev',
        },
      });
    },
  };

  /**
   * Adds slider to discover section.
   */
  Drupal.behaviors.discoverSlider = {
    attach: function (context, settings) {
      const discoverSwiper = new Swiper('.discover-slider', {
        slidesPerView: 'auto',
        spaceBetween: 16,
        navigation: {
          nextEl: '.discover-swiper-button-next',
          prevEl: '.discover-swiper-button-prev',
        },
        breakpoints: {
          768: {
            slidesPerView: 2,
            grid: {
              rows: 2,
              fill: 'row',
            },
          },
        },
      });
    },
  };

  /**
   * Adds slider to innerpage  section.
   */

  Drupal.behaviors.innerpageSlider = {
    attach: function (context, settings) {
      const innerpageSwiper = new Swiper('.innerpage-slider', {
        slidesPerView: 'auto',
        spaceBetween: 16,
        loop: true,
        navigation: {
          nextEl: '.innerpage-swiper-button-next',
          prevEl: '.innerpage-swiper-button-prev',
        },
        breakpoints: {
          768: {
            slidesPerView: 3,
          },
        },
      });
    },
  };

  Drupal.behaviors.passengerSlider = {
    attach: function (context, settings) {
      const passengerSwiper = new Swiper('.passenger-slider', {
        effect: 'coverflow',
        grabCursor: true,
        centeredSlides: true,
        slidesPerView: 1,
        initialSlide: 1,
        spaceBetween: 16,
        loop: true,
        navigation: {
          nextEl: '.passenger-swiper-button-next',
          prevEl: '.passenger-swiper-button-prev',
        },
        breakpoints: {
          992: {
            slidesPerView: 2,
            coverflowEffect: {
              stretch: 65,
              depth: 320,
              modifier: 1,
              slideShadows: false,
            },
          },
          1366: {
            slidesPerView: 2,
            coverflowEffect: {
              rotate: 0,
              stretch: 60,
              depth: 380,
              modifier: 1,
              slideShadows: false,
            },
          },
          1920: {
            slidesPerView: 2,
            coverflowEffect: {
              rotate: 0,
              stretch: 71,
              depth: 380,
              modifier: 1,
              slideShadows: false,
            },
          },
        },
      });
    },
  };

  // select for offer filter
  Drupal.behaviors.filterForMobile = {
    attach: function (context, settings) {
      $('.filter-for-mobile', context).select2({
        minimumResultsForSearch: Infinity,
        width: 'resolve',
        dropdownParent: $('.offers-for-mobile', context),
      });

      $('#offers-for-mobile', context).on('change', function () {
        $('#offers-for-mobile select option:selected', context).each(
          function () {
            selected_option = $(this).text().trim();
            $(this).attr('selected', 'selected');
            console.log(`selected ${selected_option}`);
          }
        );

        $('.offers-filter ul a', context).each(function () {
          if (selected_option == $(this).text().trim()) {
            $(this).trigger('click');
            console.log(`clicked ${selected_option}`);
          }
        });
      });

      $('#discover-for-mobile', context).on('change', function () {
        $('#discover-for-mobile select option:selected').each(function () {
          selected_option = $(this).text().trim();
          $(this).attr('selected', 'selected');
          //console.log(`selected ${selected_option}`);
        });

        $('.discover-filter ul a').each(function () {
          if (selected_option == $(this).text().trim()) {
            $(this).trigger('click');
           // console.log(`clicked ${selected_option}`);
          }
        });
      });
      //Offers and Deals Filter

      $('#offers-deals-filter-for-mobile', context).on('change', function () {
        $('#offers-deals-filter-for-mobile select option:selected', context).each(
          function () {
            selected_option = $(this).text().trim();
            $(this).attr('selected', 'selected');
            //console.log(`selected ${selected_option}`);
          }
        );

        $('.offers-deals-filter ul a', context).each(function () {
          if (selected_option == $(this).text().trim()) {
            $(this).trigger('click');
           // console.log(`clicked ${selected_option}`);
          }
        });
      });

      $('#destination-menu-for-mobile-dropdown', context).on('change', function () {
        $('#destination-menu-for-mobile-dropdown select option:selected', context).each(
          function () {
            selected_option = $(this).text().trim();
            $(this).attr('selected', 'selected');
            window.location.href = window.location.origin + $(this).attr("value");
            //console.log(`selected ${selected_option}`);
          }
        );
      });
    },
  };

  Drupal.behaviors.filterForHoliday = {
    attach: function (context, settings) {
      $('.filter-for-holiday', context).select2({
        minimumResultsForSearch: Infinity,
        width: 'resolve',
        dropdownParent: $('.offers-for-holiday-package', context),
      });
      $('#destination-filter-for-mobile', context).on('change', function () {
        $('#destination-filter-for-mobile select option:selected', context).each(
          function () {
            selected_op = $(this).text().trim();
            $(this).attr('selected', 'selected');
            //console.log(`selected ${selected_op}`);
          }
        );
        $(".form-item-field-destination-target-id ul a", context).each(function () {
          if (selected_op == $(this).text().trim()) {
            $(this).trigger('click');
            //console.log(`clicked ${selected_op}`);
          }
        });
      });

      $('#hotel-filter-for-mobile', context).on('change', function () {
        $('#hotel-filter-for-mobile select option:selected', context).each(
          function () {
            selected_oph = $(this).text().trim();
            $(this).attr('selected', 'selected');
           // console.log(`selected ${selected_oph}`);
          }
        );
        $(".form-item-field-hotel-name-target-id ul a", context).each(function () {
          if (selected_oph == $(this).text().trim()) {
            $(this).trigger('click');
            //console.log(`clicked ${selected_oph}`);
          }
        });
      });
    },
  };

  Drupal.behaviors.filterForsidebar = {
    attach: function (context, settings) {
      $('#offers-for-holiday-packages', context).on('change', function () {
        $('.filter-for-holiday-packages option:selected', context).each(
          function () {
            selected_option = $(this).text().trim();
            $(this).attr('selected', 'selected');
            //console.log(`selected ${selected_option}`);
          });
        $('#edit-field-destination-target-id option', context).each(function () {
          if (selected_option == $(this).text().trim()) {
            $(this).trigger('click');
           // console.log(`clicked ${selected_option}`);
          }
        });
      });
    },
  };

  $(document).ready(function () {
    if ($("#splashScreen").length > 0) {
      $("#splashScreen").modal("show");
      setTimeout(() => {
        $("#splashScreen").modal("hide");
      }, [7000])
    }
  });

})(jQuery, Drupal);


window.addEventListener('DOMContentLoaded',(dom)=> {
  const offer_deals_type = document.getElementById('offer_deals_type')?.getAttribute('data-tid');
  if (offer_deals_type) {
    // console.log(offer_deals_type);
    setTimeout(() => {
      let offer_deals = document.querySelector('#edit-field-offers-deals-type-target-id-all');
      if(offer_deals){
        offer_deals.classList.remove("bef-link--selected");
      }
      let filter_tid = document.querySelector(`#edit-field-offers-deals-type-target-id-${offer_deals_type}`);
      if(filter_tid){
        filter_tid.classList.add("bef-link--selected");
        filter_tid.setAttribute("selected","selected");
      }

    }, 300);
  }


  const destination_ids = document.getElementById('destination_filter');
  if (destination_ids != null) {
    destination_ids.addEventListener('change',(ch)=>{
      let idValue = ch.target.value;
      document.querySelector('#hotel_filter').value = 'All';
      document.querySelectorAll(`#hotel_filter option`)?.forEach((op)=>{
        if(op.getAttribute('data_hd') == idValue){
          op.style = "display:block;"
          op.removeAttribute('disabled')
        }
        else{
         op.style = "display:none;"
         op.setAttribute('disabled',true);
        }
      })
    })

  }

//redirect to holiday package page form node page

  const queryString = window.location.search;
  const urlParams = new URLSearchParams(queryString);
  const field_destination_target_id = urlParams.get('field_destination_target_id')
  console.log(field_destination_target_id);
  let idValue = field_destination_target_id;
  if(idValue != null) {
    document.querySelector(`#destination_filter option[value="${idValue}"]`)?.setAttribute('selected',true);
    document.querySelectorAll(`#hotel_filter option`)?.forEach((op)=>{
      console.log(idValue,op.getAttribute('data_hd'))
      if(op.getAttribute('data_hd') == idValue){
        op.style = "display:block;"
        op.removeAttribute('disabled')
      }
      else{
        op.style = "display:none;"
        op.setAttribute('disabled',true);
      }
    })
  }
});


