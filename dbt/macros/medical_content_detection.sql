{ % macro is_medical_content(message) % } case
    when lower({ { message } }) like '%medicine%'
    or lower({ { message } }) like '%drug%'
    or lower({ { message } }) like '%pharma%'
    or lower({ { message } }) like '%medication%'
    or lower({ { message } }) like '%prescription%'
    or lower({ { message } }) like '%treatment%'
    or lower({ { message } }) like '%symptom%'
    or lower({ { message } }) like '%disease%'
    or lower({ { message } }) like '%health%'
    or lower({ { message } }) like '%medical%'
    or lower({ { message } }) like '%doctor%'
    or lower({ { message } }) like '%hospital%'
    or lower({ { message } }) like '%clinic%'
    or lower({ { message } }) like '%pharmacy%'
    or lower({ { message } }) like '%cosmetic%'
    or lower({ { message } }) like '%beauty%'
    or lower({ { message } }) like '%skincare%'
    or lower({ { message } }) like '%supplement%'
    or lower({ { message } }) like '%vitamin%' then true
    else false
end { % endmacro % }