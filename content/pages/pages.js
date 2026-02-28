module.exports = {
  permalink: function(data) {
    const slug = data.page.fileSlug || "index";
    return `/${slug}.html`;
  }
};
