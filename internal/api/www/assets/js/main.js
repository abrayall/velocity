// Header scroll effect
const header = document.querySelector('.header');
const scrollThreshold = 50;

function updateHeader() {
    if (window.scrollY > scrollThreshold) {
        header.classList.add('scrolled');
    } else {
        header.classList.remove('scrolled');
    }
}

window.addEventListener('scroll', updateHeader);
updateHeader();

// Mobile menu toggle
const mobileMenuToggle = document.querySelector('.mobile-menu-toggle');
const nav = document.querySelector('.nav');

if (mobileMenuToggle) {
    mobileMenuToggle.addEventListener('click', () => {
        nav.classList.toggle('active');
    });

    // Close menu when clicking a link
    nav.querySelectorAll('a').forEach(link => {
        link.addEventListener('click', () => {
            nav.classList.remove('active');
        });
    });
}

// Smooth scrolling for anchor links
document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function(e) {
        e.preventDefault();
        const target = document.querySelector(this.getAttribute('href'));
        if (target) {
            const headerOffset = 80;
            const elementPosition = target.getBoundingClientRect().top;
            const offsetPosition = elementPosition + window.scrollY - headerOffset;

            window.scrollTo({
                top: offsetPosition,
                behavior: 'smooth'
            });
        }
    });
});

// Copy to clipboard functionality
document.querySelectorAll('.copy-btn').forEach(btn => {
    btn.addEventListener('click', async () => {
        const codeBlock = btn.closest('.code-block');
        const code = codeBlock.querySelector('code');

        // Get text content, or use data-copy attribute if available
        let textToCopy = btn.dataset.copy || code.textContent;

        // Clean up the text - remove prompt symbols and output lines
        textToCopy = textToCopy
            .split('\n')
            .filter(line => line.trim().startsWith('$') || (!line.includes('$') && btn.dataset.copy))
            .map(line => line.replace(/^\s*\$\s*/, ''))
            .join('\n')
            .trim();

        // If we have a data-copy attribute, use that instead
        if (btn.dataset.copy) {
            textToCopy = btn.dataset.copy;
        }

        try {
            await navigator.clipboard.writeText(textToCopy);
            const originalText = btn.textContent;
            btn.textContent = 'Copied!';
            setTimeout(() => {
                btn.textContent = originalText;
            }, 2000);
        } catch (err) {
            console.error('Failed to copy:', err);
        }
    });
});

// Intersection Observer for animations
const observerOptions = {
    threshold: 0.1,
    rootMargin: '0px 0px -50px 0px'
};

const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
        if (entry.isIntersecting) {
            entry.target.classList.add('animate-in');
            observer.unobserve(entry.target);
        }
    });
}, observerOptions);

// Observe elements for animation
document.querySelectorAll('.feature-card, .step, .api-example').forEach(el => {
    observer.observe(el);
});

// Active nav link based on scroll position
const sections = document.querySelectorAll('section[id]');
const navLinks = document.querySelectorAll('.nav a[href^="#"]');

function updateActiveNav() {
    const scrollPos = window.scrollY + 100;

    sections.forEach(section => {
        const sectionTop = section.offsetTop;
        const sectionHeight = section.offsetHeight;
        const sectionId = section.getAttribute('id');

        if (scrollPos >= sectionTop && scrollPos < sectionTop + sectionHeight) {
            navLinks.forEach(link => {
                link.classList.remove('active');
                if (link.getAttribute('href') === `#${sectionId}`) {
                    link.classList.add('active');
                }
            });
        }
    });
}

window.addEventListener('scroll', updateActiveNav);
updateActiveNav();

// API tabs switching
const apiTabs = document.querySelectorAll('.api-tab');
const tabContents = document.querySelectorAll('.tab-content');

apiTabs.forEach(tab => {
    tab.addEventListener('click', () => {
        const targetTab = tab.dataset.tab;

        // Update active tab
        apiTabs.forEach(t => t.classList.remove('active'));
        tab.classList.add('active');

        // Show/hide content
        tabContents.forEach(content => {
            if (content.dataset.tab === targetTab) {
                content.style.display = 'block';
            } else {
                content.style.display = 'none';
            }
        });
    });
});
